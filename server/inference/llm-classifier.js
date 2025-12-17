/**
 * LLM-based classification for ambiguous governance topics
 * Uses OpenAI API for classification when rule-based methods fail
 */

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Classification prompt with few-shot examples
const CLASSIFICATION_PROMPT = `You are a governance topic classifier for the Canton Network. Classify each topic into one of these types:

**cip** - Canton Improvement Proposals (technical protocol changes, new features, standards)
- Usually has "CIP-XXXX" or a 4-digit number like "0054"
- Examples: "CIP-0054 Add Figment as SV", "0066 - RPC Voting", "CIP Discussion: New feature"

**validator** - Validator applications and operations
- About companies becoming validators/super validators
- Examples: "Validator Approved: Figment", "Node Fortress SV Application", "Validator Operations Update"

**featured-app** - Featured applications on the network
- About apps being featured/added to the network
- Examples: "Featured App: PaymentApp", "MainNet: New Trading App", "Testnet: DeFi Protocol"

**protocol-upgrade** - Network upgrades and migrations
- About Splice versions, synchronizer migrations, hard forks
- Examples: "Migration to Splice 0.2", "Protocol Upgrade v3.0", "Synchronizer Migration"

**outcome** - Tokenomics outcome reports
- Periodic reports about network tokenomics
- Examples: "Tokenomics Outcomes - December 2024"

**other** - Doesn't fit other categories

Respond with ONLY the type (one word, lowercase).`;

/**
 * Classify a single topic using LLM
 * @param {string} subject - The topic subject line
 * @param {string} groupName - The group name where it was posted
 * @returns {Promise<{type: string, confidence: number}>}
 */
export async function classifyTopic(subject, groupName) {
  if (!OPENAI_API_KEY) {
    console.warn('⚠️ OPENAI_API_KEY not set, skipping LLM classification');
    return { type: null, confidence: 0 };
  }

  try {
    const userPrompt = `Group: ${groupName}\nSubject: ${subject}\n\nType:`;

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: CLASSIFICATION_PROMPT },
          { role: 'user', content: userPrompt }
        ],
        max_tokens: 20,
        temperature: 0,
      }),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error('❌ OpenAI API error:', response.status, error);
      return { type: null, confidence: 0 };
    }

    const data = await response.json();
    const rawType = data.choices[0]?.message?.content?.trim().toLowerCase();
    
    // Validate the type
    const validTypes = ['cip', 'validator', 'featured-app', 'protocol-upgrade', 'outcome', 'other'];
    const type = validTypes.includes(rawType) ? rawType : null;
    
    console.log(`🤖 LLM classified "${subject.slice(0, 50)}..." -> ${type}`);
    
    return { 
      type, 
      confidence: type ? 0.85 : 0,  // LLM confidence is moderately high
      llmClassified: true
    };
  } catch (error) {
    console.error('❌ LLM classification error:', error.message);
    return { type: null, confidence: 0 };
  }
}

/**
 * Batch classify multiple topics (more efficient API usage)
 * @param {Array<{subject: string, groupName: string, id: string}>} topics
 * @returns {Promise<Map<string, {type: string, confidence: number}>>}
 */
export async function classifyTopicsBatch(topics) {
  if (!OPENAI_API_KEY || topics.length === 0) {
    return new Map();
  }

  const results = new Map();
  
  // Process in small batches to avoid rate limits
  const batchSize = 5;
  for (let i = 0; i < topics.length; i += batchSize) {
    const batch = topics.slice(i, i + batchSize);
    
    // Classify each topic in parallel within the batch
    const promises = batch.map(async (topic) => {
      const result = await classifyTopic(topic.subject, topic.groupName);
      return { id: topic.id, result };
    });
    
    const batchResults = await Promise.all(promises);
    for (const { id, result } of batchResults) {
      results.set(id, result);
    }
    
    // Small delay between batches to respect rate limits
    if (i + batchSize < topics.length) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  
  return results;
}

/**
 * Check if LLM classification is available
 */
export function isLLMAvailable() {
  return !!OPENAI_API_KEY;
}
