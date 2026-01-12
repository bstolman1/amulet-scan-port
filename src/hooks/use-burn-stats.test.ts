import { describe, it, expect, vi, beforeEach } from 'vitest';

// Test the pure calculation functions by extracting them
// We'll test the logic without the React Query wrapper

interface BurnCalculationResult {
  totalBurn: number;
  trafficBurn: number;
  transferBurn: number;
  cnsBurn: number;
  preapprovalBurn: number;
}

/**
 * Parse a single exercised event for burn calculation (extracted for testing)
 */
function calculateBurnFromEvent(event: any, eventsById: Record<string, any>): Partial<BurnCalculationResult> {
  const result: Partial<BurnCalculationResult> = {
    trafficBurn: 0,
    transferBurn: 0,
    cnsBurn: 0,
    preapprovalBurn: 0,
  };

  if (event.event_type !== "exercised_event") return result;

  const choice = event.choice;
  const exerciseResult = event.exercise_result;
  const summary = exerciseResult?.summary;

  // 1. Traffic Purchases
  if (choice === "AmuletRules_BuyMemberTraffic" && summary) {
    const holdingFees = parseFloat(summary.holdingFees || "0");
    const senderChangeFee = parseFloat(summary.senderChangeFee || "0");
    const amuletPaid = parseFloat(exerciseResult.amuletPaid || "0");
    result.trafficBurn = holdingFees + senderChangeFee + amuletPaid;
  }

  // 2. Transfers
  else if (choice === "AmuletRules_Transfer" && summary) {
    const holdingFees = parseFloat(summary.holdingFees || "0");
    const senderChangeFee = parseFloat(summary.senderChangeFee || "0");
    let outputFeesTotal = 0;
    if (Array.isArray(summary.outputFees)) {
      for (const fee of summary.outputFees) {
        outputFeesTotal += parseFloat(fee || "0");
      }
    }
    result.transferBurn = holdingFees + senderChangeFee + outputFeesTotal;
  }

  // 3. Pre-approvals
  else if (
    (choice === "AmuletRules_CreateTransferPreapproval" ||
      choice === "AmuletRules_CreateExternalPartySetupProposal" ||
      choice === "TransferPreapproval_Renew") &&
    exerciseResult?.transferResult
  ) {
    const transferResult = exerciseResult.transferResult;
    const summary = transferResult.summary;

    if (summary) {
      const amuletPaid = parseFloat(exerciseResult.amuletPaid || "0");
      const holdingFees = parseFloat(summary.holdingFees || "0");
      const senderChangeFee = parseFloat(summary.senderChangeFee || "0");
      let outputFeesTotal = 0;
      if (Array.isArray(summary.outputFees)) {
        for (const fee of summary.outputFees) {
          outputFeesTotal += parseFloat(fee || "0");
        }
      }
      result.preapprovalBurn = amuletPaid + holdingFees + senderChangeFee + outputFeesTotal;
    }
  }

  return result;
}

/**
 * Calculate total burn from all events in a transaction (extracted for testing)
 */
function calculateBurnFromTransaction(transaction: any): BurnCalculationResult {
  const result: BurnCalculationResult = {
    totalBurn: 0,
    trafficBurn: 0,
    transferBurn: 0,
    cnsBurn: 0,
    preapprovalBurn: 0,
  };

  if (!transaction.events_by_id) return result;

  const eventsById = transaction.events_by_id;

  for (const eventId of Object.keys(eventsById)) {
    const event = eventsById[eventId];
    const eventBurn = calculateBurnFromEvent(event, eventsById);

    result.trafficBurn += eventBurn.trafficBurn || 0;
    result.transferBurn += eventBurn.transferBurn || 0;
    result.cnsBurn += eventBurn.cnsBurn || 0;
    result.preapprovalBurn += eventBurn.preapprovalBurn || 0;
  }

  result.totalBurn = result.trafficBurn + result.transferBurn + result.cnsBurn + result.preapprovalBurn;

  return result;
}

describe('Burn Stats Calculations', () => {
  describe('calculateBurnFromEvent', () => {
    it('returns zero for non-exercised events', () => {
      const event = { event_type: 'created_event' };
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.trafficBurn).toBe(0);
      expect(result.transferBurn).toBe(0);
      expect(result.cnsBurn).toBe(0);
      expect(result.preapprovalBurn).toBe(0);
    });

    it('calculates traffic burn correctly', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_BuyMemberTraffic',
        exercise_result: {
          amuletPaid: '10.5',
          summary: {
            holdingFees: '1.5',
            senderChangeFee: '0.25',
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.trafficBurn).toBe(12.25); // 10.5 + 1.5 + 0.25
      expect(result.transferBurn).toBe(0);
    });

    it('calculates transfer burn correctly with multiple output fees', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_Transfer',
        exercise_result: {
          summary: {
            holdingFees: '2.0',
            senderChangeFee: '0.5',
            outputFees: ['0.1', '0.2', '0.3'],
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.transferBurn).toBe(3.1); // 2.0 + 0.5 + 0.6
      expect(result.trafficBurn).toBe(0);
    });

    it('calculates preapproval burn correctly', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_CreateTransferPreapproval',
        exercise_result: {
          amuletPaid: '5.0',
          transferResult: {
            summary: {
              holdingFees: '0.5',
              senderChangeFee: '0.1',
              outputFees: ['0.05'],
            },
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.preapprovalBurn).toBe(5.65); // 5.0 + 0.5 + 0.1 + 0.05
    });

    it('handles missing summary gracefully', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_Transfer',
        exercise_result: {},
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.transferBurn).toBe(0);
    });

    it('handles missing fee values as zero', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_BuyMemberTraffic',
        exercise_result: {
          summary: {
            // Missing all fee fields
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.trafficBurn).toBe(0);
    });
  });

  describe('calculateBurnFromTransaction', () => {
    it('returns zero for empty transaction', () => {
      const result = calculateBurnFromTransaction({});
      
      expect(result.totalBurn).toBe(0);
      expect(result.trafficBurn).toBe(0);
      expect(result.transferBurn).toBe(0);
      expect(result.cnsBurn).toBe(0);
      expect(result.preapprovalBurn).toBe(0);
    });

    it('aggregates burn from multiple events', () => {
      const transaction = {
        events_by_id: {
          event1: {
            event_type: 'exercised_event',
            choice: 'AmuletRules_BuyMemberTraffic',
            exercise_result: {
              amuletPaid: '10.0',
              summary: {
                holdingFees: '1.0',
                senderChangeFee: '0.5',
              },
            },
          },
          event2: {
            event_type: 'exercised_event',
            choice: 'AmuletRules_Transfer',
            exercise_result: {
              summary: {
                holdingFees: '2.0',
                senderChangeFee: '0.25',
                outputFees: ['0.1'],
              },
            },
          },
        },
      };
      
      const result = calculateBurnFromTransaction(transaction);
      
      expect(result.trafficBurn).toBe(11.5); // 10 + 1 + 0.5
      expect(result.transferBurn).toBe(2.35); // 2 + 0.25 + 0.1
      expect(result.totalBurn).toBe(13.85); // 11.5 + 2.35
    });

    it('ignores non-burn events in transaction', () => {
      const transaction = {
        events_by_id: {
          event1: {
            event_type: 'created_event',
            template_id: 'Splice.Amulet:Amulet',
          },
          event2: {
            event_type: 'archived_event',
            template_id: 'Splice.Amulet:Amulet',
          },
        },
      };
      
      const result = calculateBurnFromTransaction(transaction);
      
      expect(result.totalBurn).toBe(0);
    });

    it('handles mixed event types correctly', () => {
      const transaction = {
        events_by_id: {
          event1: {
            event_type: 'created_event', // Ignored
          },
          event2: {
            event_type: 'exercised_event',
            choice: 'AmuletRules_Transfer',
            exercise_result: {
              summary: {
                holdingFees: '1.0',
                senderChangeFee: '0.0',
                outputFees: [],
              },
            },
          },
          event3: {
            event_type: 'archived_event', // Ignored
          },
        },
      };
      
      const result = calculateBurnFromTransaction(transaction);
      
      expect(result.transferBurn).toBe(1.0);
      expect(result.totalBurn).toBe(1.0);
    });
  });

  describe('Edge Cases', () => {
    it('handles NaN values gracefully', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_Transfer',
        exercise_result: {
          summary: {
            holdingFees: 'invalid',
            senderChangeFee: 'NaN',
            outputFees: ['notanumber'],
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      // parseFloat of invalid strings returns NaN, which propagates
      expect(Number.isNaN(result.transferBurn)).toBe(true);
    });

    it('handles very large numbers', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_BuyMemberTraffic',
        exercise_result: {
          amuletPaid: '999999999999.999999',
          summary: {
            holdingFees: '0.000001',
            senderChangeFee: '0.000001',
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.trafficBurn).toBeGreaterThan(999999999999);
    });

    it('handles empty outputFees array', () => {
      const event = {
        event_type: 'exercised_event',
        choice: 'AmuletRules_Transfer',
        exercise_result: {
          summary: {
            holdingFees: '1.0',
            senderChangeFee: '0.5',
            outputFees: [],
          },
        },
      };
      
      const result = calculateBurnFromEvent(event, {});
      
      expect(result.transferBurn).toBe(1.5);
    });
  });
});
