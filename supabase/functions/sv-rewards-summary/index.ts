// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Summarizes claimed, expired, and unclaimed SV rewards based on SvRewardCoupon activity

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS'
};

interface PaginationKey {
  last_migration_id: number;
  last_record_time: string;
}

interface SvRewardCoupon {
  contractId: string;
  beneficiary: string;
  weight: number;
  round: number;
  expiresAt: string;
}

interface MiningRound {
  round: number;
  issuancePerSvReward: string;
}

interface RewardSummary {
  totalSuperValidators: number;
  totalRewardCoupons: number;
  claimedCount: number;
  claimedAmount: string;
  expiredCount: number;
  expiredAmount: string;
  unclaimedCount: number;
  estimatedUnclaimedAmount: string;
  timeRangeStart: string;
  timeRangeEnd: string;
}

interface AppState {
  activeRewards: Map<string, SvRewardCoupon>;
  issuingRounds: Map<number, MiningRound>;
  closedRounds: Map<number, MiningRound>;
  expiredCount: number;
  expiredAmount: number;
  claimedCount: number;
  claimedAmount: number;
}

const TEMPLATE_QUALIFIED_NAMES = {
  svRewardCoupon: 'Splice.Amulet:SvRewardCoupon',
  issuingMiningRound: 'Splice.Round:IssuingMiningRound',
  closedMiningRound: 'Splice.Round:ClosedMiningRound',
};

class DamlDecimal {
  value: number;

  constructor(value: string | number) {
    this.value = typeof value === 'string' ? parseFloat(value) : value;
  }

  multiply(other: DamlDecimal): DamlDecimal {
    return new DamlDecimal(this.value * other.value);
  }

  add(other: DamlDecimal): DamlDecimal {
    return new DamlDecimal(this.value + other.value);
  }

  toFixed(decimals: number = 10): string {
    return this.value.toFixed(decimals);
  }
}

// Fetch transactions from scan API with pagination
async function fetchTransactions(
  scanUrl: string,
  paginationKey: PaginationKey | null,
  pageSize: number = 100
): Promise<any[]> {
  const payload: any = { page_size: pageSize };

  if (paginationKey) {
    payload.after = {
      after_record_time: paginationKey.last_record_time,
      after_migration_id: paginationKey.last_migration_id
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60000); // 60 second timeout

  try {
    const response = await fetch(`${scanUrl}/api/scan/v2/updates`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal
    });

    clearTimeout(timeout);

    if (!response.ok) {
      throw new Error(`Failed to fetch transactions: ${response.statusText}`);
    }

    const data = await response.json();
    return data.transactions || [];
  } catch (error) {
    clearTimeout(timeout);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error('Request timeout: Scan API did not respond within 60 seconds');
    }
    throw error;
  }
}

function parseTemplateId(templateId: string): { packageId: string; qualifiedName: string } {
  const [packageId, qualifiedName] = templateId.split(':', 2);
  return { packageId, qualifiedName };
}

function getLfValue(value: any, path: string[]): any {
  let current = value;
  for (const key of path) {
    if (current && typeof current === 'object' && key in current) {
      current = current[key];
    } else {
      return null;
    }
  }
  return current;
}

// Calculate reward amount based on weight and issuance
function calculateRewardAmount(weight: number, issuancePerSvReward: string, alreadyMintedWeight: number): number {
  const availableWeight = Math.max(0, weight - alreadyMintedWeight);
  const issuance = new DamlDecimal(issuancePerSvReward);
  const amount = new DamlDecimal(availableWeight).multiply(issuance);
  return amount.value;
}

// Process created events for mining rounds
function processRoundCreated(
  event: any,
  state: AppState
): void {
  const { qualifiedName } = parseTemplateId(event.template_id);
  const payload = event.create_arguments;

  if (qualifiedName === TEMPLATE_QUALIFIED_NAMES.issuingMiningRound) {
    const round = parseInt(getLfValue(payload, ['round', 'number']));
    const issuancePerSvReward = getLfValue(payload, ['issuancePerSvRewardCoupon']);

    if (round && issuancePerSvReward) {
      state.issuingRounds.set(round, { round, issuancePerSvReward });
    }
  } else if (qualifiedName === TEMPLATE_QUALIFIED_NAMES.closedMiningRound) {
    const round = parseInt(getLfValue(payload, ['round', 'number']));
    const issuancePerSvReward = getLfValue(payload, ['issuancePerSvRewardCoupon']);

    if (round && issuancePerSvReward) {
      state.closedRounds.set(round, { round, issuancePerSvReward });
    }
  }
}

// Process created events for reward coupons
function processCouponCreated(
  event: any,
  transaction: any,
  state: AppState,
  beneficiary: string,
  endRecordTime: Date
): void {
  const { qualifiedName } = parseTemplateId(event.template_id);

  if (qualifiedName !== TEMPLATE_QUALIFIED_NAMES.svRewardCoupon) return;

  const recordTime = new Date(transaction.record_time);
  if (recordTime > endRecordTime) return;

  const payload = event.create_arguments;
  const rewardBeneficiary = getLfValue(payload, ['beneficiary']);

  if (rewardBeneficiary !== beneficiary) return;

  const round = parseInt(getLfValue(payload, ['round', 'number']));
  const weight = parseInt(getLfValue(payload, ['weight']));
  const expiresAt = getLfValue(payload, ['expiresAt']);

  state.activeRewards.set(event.contract_id, {
    contractId: event.contract_id,
    beneficiary: rewardBeneficiary,
    weight,
    round,
    expiresAt
  });
}

// Process exercised events for reward coupons
function processCouponExercised(
  event: any,
  state: AppState,
  weight: number,
  alreadyMintedWeight: number
): void {
  const { qualifiedName } = parseTemplateId(event.template_id);

  if (qualifiedName !== TEMPLATE_QUALIFIED_NAMES.svRewardCoupon) return;

  const choiceName = event.choice;
  const coupon = state.activeRewards.get(event.contract_id);

  if (!coupon) return;
  if (coupon.weight !== weight) return;

  const isExpired = choiceName === 'SvRewardCoupon_DsoExpire';
  const isClaimed = choiceName === 'SvRewardCoupon_ArchiveAsBeneficiary';

  if (!isExpired && !isClaimed) return;

  state.activeRewards.delete(event.contract_id);

  const rounds = isExpired ? state.closedRounds : state.issuingRounds;
  const miningRound = rounds.get(coupon.round);

  if (miningRound) {
    const amount = calculateRewardAmount(weight, miningRound.issuancePerSvReward, alreadyMintedWeight);

    if (isExpired) {
      state.expiredCount++;
      state.expiredAmount += amount;
    } else {
      state.claimedCount++;
      state.claimedAmount += amount;
    }
  }
}

// Process all events in a transaction recursively
function processEvents(
  eventIds: string[],
  eventsById: Record<string, any>,
  transaction: any,
  state: AppState,
  beneficiary: string,
  endRecordTime: Date,
  weight: number,
  alreadyMintedWeight: number,
  phase: 'rounds' | 'coupons'
): void {
  for (const eventId of eventIds) {
    const event = eventsById[eventId];
    if (!event) continue;

    if (event.create_arguments) {
      if (phase === 'rounds') {
        processRoundCreated(event, state);
      } else {
        processCouponCreated(event, transaction, state, beneficiary, endRecordTime);
      }
    } else if (event.choice && phase === 'coupons') {
      processCouponExercised(event, state, weight, alreadyMintedWeight);
    }

    // Process child events recursively
    if (event.child_event_ids && event.child_event_ids.length > 0) {
      processEvents(
        event.child_event_ids,
        eventsById,
        transaction,
        state,
        beneficiary,
        endRecordTime,
        weight,
        alreadyMintedWeight,
        phase
      );
    }
  }
}

// Main calculation function
async function calculateRewardsSummary(
  scanUrl: string,
  beneficiary: string,
  beginRecordTime: string,
  endRecordTime: string,
  beginMigrationId: number,
  weight: number,
  alreadyMintedWeight: number,
  gracePeriodMinutes: number
): Promise<RewardSummary> {
  console.log(`Starting reward summary calculation for beneficiary: ${beneficiary}`);
  console.log('Request parameters:', {
    beneficiary,
    beginRecordTime,
    endRecordTime,
    beginMigrationId,
    weight,
    alreadyMintedWeight,
    gracePeriodMinutes,
    scanUrl
  });

  const beginTime = new Date(beginRecordTime);
  const endTime = new Date(endRecordTime);
  const graceTime = new Date(endTime.getTime() + gracePeriodMinutes * 60 * 1000);

  console.log(`Time range: ${beginTime.toISOString()} to ${endTime.toISOString()} (grace: ${graceTime.toISOString()})`);

  const state: AppState = {
    activeRewards: new Map(),
    issuingRounds: new Map(),
    closedRounds: new Map(),
    expiredCount: 0,
    expiredAmount: 0,
    claimedCount: 0,
    claimedAmount: 0
  };

  const PAGE_SIZE = 50; // Reduced for faster responses
  const MAX_BATCHES = 100; // Prevent infinite loops
  let totalProcessed = 0;
  let batchCount = 0;

  // Phase 1: Collect mining rounds with grace period
  console.log('Phase 1: Collecting mining rounds...');
  let collectingRounds = true;
  let roundsPaginationKey: PaginationKey | null = {
    last_migration_id: beginMigrationId,
    last_record_time: beginRecordTime
  };

  while (collectingRounds && batchCount < MAX_BATCHES) {
    console.log(`Fetching rounds batch ${batchCount + 1}...`);
    const batch = await fetchTransactions(scanUrl, roundsPaginationKey, PAGE_SIZE);

    if (batch.length === 0) break;
    batchCount++;

    for (const tx of batch) {
      const recordTime = new Date(tx.record_time);
      if (recordTime > graceTime) {
        collectingRounds = false;
        break;
      }

      if (tx.root_event_ids) {
        processEvents(
          tx.root_event_ids,
          tx.events_by_id,
          tx,
          state,
          beneficiary,
          endTime,
          weight,
          alreadyMintedWeight,
          'rounds'
        );
      }
    }

    if (batch.length < PAGE_SIZE) break;

    const lastTx = batch[batch.length - 1];
    roundsPaginationKey = {
      last_migration_id: lastTx.migration_id,
      last_record_time: lastTx.record_time
    };
  }

  console.log(`Collected ${state.issuingRounds.size} issuing rounds and ${state.closedRounds.size} closed rounds`);

  // Phase 2: Process reward coupons
  console.log('Phase 2: Processing reward coupons...');
  let paginationKey: PaginationKey | null = {
    last_migration_id: beginMigrationId,
    last_record_time: beginRecordTime
  };

  let couponBatchCount = 0;
  while (couponBatchCount < MAX_BATCHES) {
    couponBatchCount++;
    console.log(`Fetching coupons batch ${couponBatchCount}...`);
    const batch = await fetchTransactions(scanUrl, paginationKey, PAGE_SIZE);

    if (batch.length === 0) {
      console.log('No more transactions to process');
      break;
    }

    let shouldStop = false;
    for (const tx of batch) {
      const recordTime = new Date(tx.record_time);

      if (recordTime > endTime) {
        shouldStop = true;
        break;
      }

      if (tx.root_event_ids) {
        processEvents(
          tx.root_event_ids,
          tx.events_by_id,
          tx,
          state,
          beneficiary,
          endTime,
          weight,
          alreadyMintedWeight,
          'coupons'
        );
      }

      totalProcessed++;
    }

    if (batch.length > 0) {
      const lastTx = batch[batch.length - 1];
      paginationKey = {
        last_migration_id: lastTx.migration_id,
        last_record_time: lastTx.record_time
      };
    }

    if (shouldStop || batch.length < PAGE_SIZE) {
      console.log(`Stopping: shouldStop=${shouldStop}, batchSize=${batch.length}`);
      break;
    }

    // Log progress every 5 batches
    if (couponBatchCount % 5 === 0) {
      console.log(`Progress: ${couponBatchCount} batches, ${totalProcessed} transactions processed`);
      console.log(`Active: ${state.activeRewards.size}, Claimed: ${state.claimedCount}, Expired: ${state.expiredCount}`);
    }
  }

  console.log(`Processed ${totalProcessed} transactions in ${couponBatchCount} coupon batches`);
  console.log(`Active rewards: ${state.activeRewards.size}`);
  console.log(`Claimed: ${state.claimedCount}, Expired: ${state.expiredCount}`);

  if (couponBatchCount >= MAX_BATCHES) {
    console.warn(`Warning: Reached maximum batch limit (${MAX_BATCHES}). Results may be incomplete.`);
  }

  return buildSummary(state, beginRecordTime, endRecordTime);
}

function buildSummary(
  state: AppState,
  beginRecordTime: string,
  endRecordTime: string
): RewardSummary {
  const unclaimedCount = state.activeRewards.size;
  const totalCoupons = state.claimedCount + state.expiredCount + unclaimedCount;

  // Estimate unclaimed amount
  const avgAmountPerCoupon = state.claimedCount > 0
    ? state.claimedAmount / state.claimedCount
    : 0;
  const estimatedUnclaimedAmount = avgAmountPerCoupon * unclaimedCount;

  return {
    totalSuperValidators: 13,
    totalRewardCoupons: totalCoupons,
    claimedCount: state.claimedCount,
    claimedAmount: state.claimedAmount.toFixed(10),
    expiredCount: state.expiredCount,
    expiredAmount: state.expiredAmount.toFixed(10),
    unclaimedCount,
    estimatedUnclaimedAmount: estimatedUnclaimedAmount.toFixed(10),
    timeRangeStart: beginRecordTime,
    timeRangeEnd: endRecordTime,
  };
}

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const {
      beneficiary,
      beginRecordTime,
      endRecordTime,
      beginMigrationId,
      weight,
      alreadyMintedWeight,
      gracePeriodMinutes = 60,
      scanUrl = 'https://scan.sv-1.global.canton.network.sync.global'
    } = await req.json();

    if (!beneficiary || !beginRecordTime || !endRecordTime || beginMigrationId === undefined || weight === undefined || alreadyMintedWeight === undefined) {
      return new Response(
        JSON.stringify({
          error: 'Missing required parameters: beneficiary, beginRecordTime, endRecordTime, beginMigrationId, weight, alreadyMintedWeight'
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const summary = await calculateRewardsSummary(
      scanUrl,
      beneficiary,
      beginRecordTime,
      endRecordTime,
      beginMigrationId,
      weight,
      alreadyMintedWeight,
      gracePeriodMinutes
    );

    return new Response(
      JSON.stringify(summary),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Error calculating rewards summary:', error);
    return new Response(
      JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error occurred',
        details: error instanceof Error ? error.stack : undefined
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
