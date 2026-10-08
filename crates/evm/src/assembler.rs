// Copyright 2025 Circle Internet Group, Inc. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate alloc;

use alloc::sync::Arc;
use alloy_consensus::{
    proofs::{calculate_receipt_root, calculate_transaction_root},
    Block, BlockBody, BlockHeader, Header, TxReceipt, EMPTY_OMMER_ROOT_HASH,
};
use alloy_eips::{eip4895::Withdrawals, merge::BEACON_NONCE};
use alloy_evm::block::BlockExecutorFactory;
use arc_execution_config::{chainspec::ArcChainSpec, gas_fee::encode_base_fee_to_bytes};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_ethereum::provider::BlockExecutionResult;
use reth_ethereum_primitives::{Receipt, TransactionSigned};
use reth_evm::execute::{BlockAssembler, BlockAssemblerInput, BlockExecutionError};
use reth_primitives_traits::logs_bloom;
use revm::context::Block as RevmBlockContext;
use revm_primitives::B256;

use crate::evm::ArcBlockExecutionCtx;
use arc_precompiles::system_accounting::{
    compute_gas_values_storage_slot, unpack_gas_values_from_storage, SYSTEM_ACCOUNTING_ADDRESS,
};

#[derive(Debug, Clone)]
pub struct ArcBlockAssembler<ChainSpec = ArcChainSpec> {
    chain_spec: Arc<ChainSpec>,
}

impl<ChainSpec> ArcBlockAssembler<ChainSpec> {
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

impl<F, ChainSpec> BlockAssembler<F> for ArcBlockAssembler<ChainSpec>
where
    F: for<'a> BlockExecutorFactory<
        ExecutionCtx<'a> = ArcBlockExecutionCtx<'a>,
        Transaction = TransactionSigned,
        Receipt = Receipt,
    >,
    ChainSpec: EthChainSpec + EthereumHardforks,
{
    type Block = Block<TransactionSigned>;

    fn assemble_block(
        &self,
        mut input: BlockAssemblerInput<'_, '_, F, <Self::Block as reth_node_api::Block>::Header>,
    ) -> Result<Self::Block, BlockExecutionError> {
        // Loading the gas values from the system accounting contract and insert to the extra data.
        let block_number_result =
            input
                .evm_env
                .block_env()
                .number()
                .try_into()
                .inspect_err(|err| {
                    tracing::warn!("Failed to convert block number to u64: {}", err);
                });

        if let Ok(block_number) = block_number_result {
            let slot = compute_gas_values_storage_slot(block_number);

            // If the state changed, read the new value from bundle_state.
            let mut value =
                if let Some(account) = input.bundle_state.account(&SYSTEM_ACCOUNTING_ADDRESS) {
                    account.storage_slot(slot.into())
                } else {
                    None
                };

            // Read from state provider if the state is not changed.
            if value.is_none() {
                value = input
                    .state_provider
                    .storage(SYSTEM_ACCOUNTING_ADDRESS, slot)
                    .unwrap_or(None)
            }

            if let Some(value) = value {
                let gas_values = unpack_gas_values_from_storage(B256::from(value));
                if gas_values.nextBaseFee != 0 {
                    input.execution_ctx.extra_data =
                        encode_base_fee_to_bytes(gas_values.nextBaseFee);
                }
            } else {
                tracing::warn!("Gas value not found for block number: {}", block_number);
            }
        }

        let BlockAssemblerInput {
            evm_env,
            execution_ctx,
            parent,
            transactions,
            output:
                BlockExecutionResult {
                    receipts,
                    requests,
                    gas_used,
                    blob_gas_used,
                },
            state_root,
            ..
        } = input;
        let ctx = execution_ctx.inner;
        let timestamp = evm_env.block_env.timestamp().saturating_to();
        let transactions_root = calculate_transaction_root(&transactions);
        let receipts_root = calculate_receipt_root(
            &receipts
                .iter()
                .map(|receipt| receipt.with_bloom_ref())
                .collect::<Vec<_>>(),
        );
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|receipt| receipt.logs()));
        let withdrawals = self
            .chain_spec
            .is_shanghai_active_at_timestamp(timestamp)
            .then(|| Withdrawals::new(ctx.withdrawals.map(|w| w.into_owned()).unwrap_or_default()));
        let withdrawals_root = withdrawals
            .as_deref()
            .map(|withdrawals| alloy_consensus::proofs::calculate_withdrawals_root(withdrawals));
        let requests_hash = self
            .chain_spec
            .is_prague_active_at_timestamp(timestamp)
            .then(|| requests.requests_hash());
        let mut excess_blob_gas = None;
        let mut block_blob_gas_used = None;
        if self.chain_spec.is_cancun_active_at_timestamp(timestamp) {
            block_blob_gas_used = Some(*blob_gas_used);
            excess_blob_gas = if self
                .chain_spec
                .is_cancun_active_at_timestamp(parent.timestamp)
            {
                parent.maybe_next_block_excess_blob_gas(
                    self.chain_spec.blob_params_at_timestamp(timestamp),
                )
            } else {
                Some(
                    alloy_eips::eip7840::BlobParams::cancun()
                        .next_block_excess_blob_gas_osaka(0, 0, 0),
                )
            };
        }
        Ok(Block {
            header: Header {
                parent_hash: ctx.parent_hash,
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                beneficiary: evm_env.block_env.beneficiary(),
                state_root,
                transactions_root,
                receipts_root,
                withdrawals_root,
                logs_bloom,
                timestamp,
                mix_hash: evm_env.block_env.prevrandao().unwrap_or_default(),
                nonce: BEACON_NONCE.into(),
                base_fee_per_gas: Some(evm_env.block_env.basefee()),
                number: evm_env.block_env.number().saturating_to(),
                gas_limit: evm_env.block_env.gas_limit(),
                difficulty: evm_env.block_env.difficulty(),
                gas_used: *gas_used,
                extra_data: ctx.extra_data,
                parent_beacon_block_root: ctx.parent_beacon_block_root,
                blob_gas_used: block_blob_gas_used,
                excess_blob_gas,
                requests_hash,
                block_access_list_hash: None,
                slot_number: None,
            },
            body: BlockBody {
                transactions,
                ommers: Default::default(),
                withdrawals,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use arc_execution_config::chainspec::LOCAL_DEV;

    #[test]
    fn block_assembler_creation() {
        let chain_spec = LOCAL_DEV.clone();
        let assembler = ArcBlockAssembler::new(chain_spec.clone());

        // Verify the inner assembler's chain_spec points to the same
        assert!(Arc::ptr_eq(&assembler.chain_spec, &chain_spec));
    }
}
