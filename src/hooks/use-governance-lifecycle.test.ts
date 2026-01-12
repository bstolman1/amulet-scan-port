import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Test the stage inference logic used in governance lifecycle
// These are the core classification rules extracted for unit testing

type Stage = 'initial' | 'review' | 'voting' | 'approved' | 'executed' | 'rejected' | 'expired' | 'unknown';

interface VoteInfo {
  partyId: string;
  accepted: boolean;
  reason?: string;
  timestamp: string;
}

interface ProposalState {
  createdAt: string;
  expiresAt: string;
  votes: VoteInfo[];
  acceptedWeight: number;
  requiredWeight: number;
  isExecuted: boolean;
  isRejected: boolean;
}

/**
 * Infer the stage of a governance proposal (extracted for testing)
 */
function inferProposalStage(state: ProposalState, now: Date = new Date()): Stage {
  const expiresAt = new Date(state.expiresAt);
  const createdAt = new Date(state.createdAt);
  
  // Already executed
  if (state.isExecuted) {
    return 'executed';
  }
  
  // Explicitly rejected
  if (state.isRejected) {
    return 'rejected';
  }
  
  // Check if expired
  if (now > expiresAt) {
    // Even if expired, check if it had enough votes (should have been executed)
    if (state.acceptedWeight >= state.requiredWeight) {
      return 'approved'; // Had enough votes but wasn't executed yet
    }
    return 'expired';
  }
  
  // Has enough votes - approved
  if (state.acceptedWeight >= state.requiredWeight) {
    return 'approved';
  }
  
  // Has some votes - in voting phase
  if (state.votes.length > 0) {
    return 'voting';
  }
  
  // Recently created (within 24h) - initial phase
  const hoursSinceCreation = (now.getTime() - createdAt.getTime()) / (1000 * 60 * 60);
  if (hoursSinceCreation < 24) {
    return 'initial';
  }
  
  // Older but no votes yet - review phase
  return 'review';
}

/**
 * Calculate vote summary from votes array
 */
function calculateVoteSummary(votes: VoteInfo[]): { accepted: number; rejected: number; total: number } {
  return {
    accepted: votes.filter(v => v.accepted).length,
    rejected: votes.filter(v => !v.accepted).length,
    total: votes.length,
  };
}

describe('Governance Lifecycle Stage Inference', () => {
  const baseState: ProposalState = {
    createdAt: '2025-01-10T10:00:00Z',
    expiresAt: '2025-01-17T10:00:00Z',
    votes: [],
    acceptedWeight: 0,
    requiredWeight: 50000,
    isExecuted: false,
    isRejected: false,
  };

  describe('inferProposalStage', () => {
    it('returns executed when proposal is executed', () => {
      const state = { ...baseState, isExecuted: true };
      const stage = inferProposalStage(state);
      expect(stage).toBe('executed');
    });

    it('returns rejected when proposal is rejected', () => {
      const state = { ...baseState, isRejected: true };
      const stage = inferProposalStage(state);
      expect(stage).toBe('rejected');
    });

    it('returns expired when past expiration with insufficient votes', () => {
      const state = { ...baseState, acceptedWeight: 25000 };
      const futureDate = new Date('2025-01-20T10:00:00Z');
      const stage = inferProposalStage(state, futureDate);
      expect(stage).toBe('expired');
    });

    it('returns approved when past expiration but had enough votes', () => {
      const state = { ...baseState, acceptedWeight: 50000 };
      const futureDate = new Date('2025-01-20T10:00:00Z');
      const stage = inferProposalStage(state, futureDate);
      expect(stage).toBe('approved');
    });

    it('returns approved when has enough votes before expiration', () => {
      const state = { ...baseState, acceptedWeight: 60000 };
      const currentDate = new Date('2025-01-12T10:00:00Z');
      const stage = inferProposalStage(state, currentDate);
      expect(stage).toBe('approved');
    });

    it('returns voting when has votes but not enough', () => {
      const state = {
        ...baseState,
        acceptedWeight: 20000,
        votes: [
          { partyId: 'party1', accepted: true, timestamp: '2025-01-11T10:00:00Z' },
        ],
      };
      const currentDate = new Date('2025-01-12T10:00:00Z');
      const stage = inferProposalStage(state, currentDate);
      expect(stage).toBe('voting');
    });

    it('returns initial when created within 24 hours and no votes', () => {
      const state = { ...baseState };
      const sameDay = new Date('2025-01-10T20:00:00Z');
      const stage = inferProposalStage(state, sameDay);
      expect(stage).toBe('initial');
    });

    it('returns review when older than 24h with no votes', () => {
      const state = { ...baseState };
      const laterDate = new Date('2025-01-12T10:00:00Z');
      const stage = inferProposalStage(state, laterDate);
      expect(stage).toBe('review');
    });
  });

  describe('Stage Priority', () => {
    it('executed takes priority over approved', () => {
      const state = {
        ...baseState,
        isExecuted: true,
        acceptedWeight: 60000, // Has enough votes
      };
      expect(inferProposalStage(state)).toBe('executed');
    });

    it('rejected takes priority over expired', () => {
      const state = {
        ...baseState,
        isRejected: true,
      };
      const futureDate = new Date('2025-01-20T10:00:00Z');
      expect(inferProposalStage(state, futureDate)).toBe('rejected');
    });

    it('executed takes priority over rejected', () => {
      const state = {
        ...baseState,
        isExecuted: true,
        isRejected: true, // Edge case: both flags set
      };
      expect(inferProposalStage(state)).toBe('executed');
    });
  });

  describe('Edge Cases', () => {
    it('handles exact expiration boundary', () => {
      const state = { ...baseState };
      const exactExpiry = new Date('2025-01-17T10:00:00Z');
      // At exact expiry time, should be expired
      const stage = inferProposalStage(state, exactExpiry);
      expect(stage).toBe('expired');
    });

    it('handles exact 24-hour boundary for initial/review', () => {
      const state = { ...baseState };
      const exactly24h = new Date('2025-01-11T10:00:00Z');
      // At exactly 24h, should transition to review
      const stage = inferProposalStage(state, exactly24h);
      expect(stage).toBe('review');
    });

    it('handles zero required weight', () => {
      const state = {
        ...baseState,
        requiredWeight: 0,
        acceptedWeight: 0,
      };
      // 0 >= 0 is true, so should be approved
      expect(inferProposalStage(state)).toBe('approved');
    });

    it('handles negative weight (invalid but should not crash)', () => {
      const state = {
        ...baseState,
        acceptedWeight: -100,
        requiredWeight: 50000,
      };
      const currentDate = new Date('2025-01-12T10:00:00Z');
      expect(inferProposalStage(state, currentDate)).toBe('review');
    });
  });
});

describe('Vote Summary Calculation', () => {
  it('correctly counts accepted and rejected votes', () => {
    const votes: VoteInfo[] = [
      { partyId: 'party1', accepted: true, timestamp: '2025-01-11T10:00:00Z' },
      { partyId: 'party2', accepted: true, timestamp: '2025-01-11T11:00:00Z' },
      { partyId: 'party3', accepted: false, timestamp: '2025-01-11T12:00:00Z' },
    ];
    
    const summary = calculateVoteSummary(votes);
    
    expect(summary.accepted).toBe(2);
    expect(summary.rejected).toBe(1);
    expect(summary.total).toBe(3);
  });

  it('handles empty votes array', () => {
    const summary = calculateVoteSummary([]);
    
    expect(summary.accepted).toBe(0);
    expect(summary.rejected).toBe(0);
    expect(summary.total).toBe(0);
  });

  it('handles all accepted votes', () => {
    const votes: VoteInfo[] = [
      { partyId: 'party1', accepted: true, timestamp: '2025-01-11T10:00:00Z' },
      { partyId: 'party2', accepted: true, timestamp: '2025-01-11T11:00:00Z' },
    ];
    
    const summary = calculateVoteSummary(votes);
    
    expect(summary.accepted).toBe(2);
    expect(summary.rejected).toBe(0);
  });

  it('handles all rejected votes', () => {
    const votes: VoteInfo[] = [
      { partyId: 'party1', accepted: false, timestamp: '2025-01-11T10:00:00Z' },
      { partyId: 'party2', accepted: false, timestamp: '2025-01-11T11:00:00Z' },
    ];
    
    const summary = calculateVoteSummary(votes);
    
    expect(summary.accepted).toBe(0);
    expect(summary.rejected).toBe(2);
  });
});
