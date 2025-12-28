/**
 * LLM-based classification for ambiguous governance topics
 * Uses OpenAI API for classification when rule-based methods fail
 * Enhanced to analyze topic excerpts for smarter categorization
 */

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Enhanced classification prompt with excerpt analysis
const CLASSIFICATION_PROMPT = `You are an expert governance topic classifier for the Canton Network / Splice ecosystem. Your job is to analyze topic subjects AND their content excerpts to classify each topic accurately.

## Classification Types:

**cip** - Canton Improvement Proposals
- Technical protocol changes, standards, new features
- Usually contains "CIP-XXXX", "CIP #XX", or 4-digit proposal numbers like "0054"
- Discusses protocol specifications, governance rules, voting parameters
- Examples: "CIP-0054 Add Figment as SV", "0066 - RPC Voting", "CIP Discussion: Validator onboarding"

**validator** - Validator/Super-Validator applications and operations
- About companies becoming validators or super-validators
- Node operators, infrastructure providers joining the network
- Validator approvals, applications, onboarding discussions
- Look for company names with terms like "SV", "validator", "operator", "node"
- Examples: "Validator Approved: Figment", "Node Fortress SV Application", "New validator: Blockdaemon"

**featured-app** - Featured applications on the network
- Apps being reviewed, featured, or added to Canton/Splice
- Usually mentions app/product names with terms like "featured", "app", "application"
- May reference testnet or mainnet deployments
- Examples: "Featured App: PaymentApp", "New Featured App Request: Rhein Finance", "Testnet: DeFi Protocol"

**protocol-upgrade** - Network upgrades and migrations
- Splice version upgrades (e.g., Splice 0.2.x)
- Synchronizer migrations, hard forks, breaking changes
- Global domain participant upgrades
- Examples: "Migration to Splice 0.2", "Protocol Upgrade v3.0", "Synchronizer Migration Required"

**outcome** - Tokenomics outcome reports
- Periodic reports about network tokenomics
- Round outcomes, CC statistics, network metrics
- Examples: "Tokenomics Outcomes - December 2024", "Round 123 Outcome Report"

**other** - Only use this if topic genuinely doesn't fit ANY other category
- Administrative announcements unrelated to governance
- General discussions without clear governance implications

## Analysis Guidelines:
1. Read BOTH the subject line AND the excerpt content carefully
2. Look for key entity names (companies, apps, protocols) in the excerpt
3. Consider the context: where was it posted, what is being discussed?
4. When in doubt between validator/featured-app, check if it's infrastructure (validator) vs application (featured-app)
5. CIPs almost always have a number - if no number but discusses protocol changes, still use "cip"

Respond with ONLY the type (one word, lowercase). Never respond with explanations.`;

/**
 * Classify a single topic using LLM with excerpt analysis
 * @param {string} subject - The topic subject line
 * @param {string} groupName - The group name where it was posted
 * @param {string} excerpt - Content excerpt (first 500 chars of topic body)
 * @returns {Promise<{type: string, confidence: number, llmClassified: boolean}>}
 */
export async function classifyTopic(subject, groupName, excerpt = '') {
  if (!OPENAI_API_KEY) {
    console.warn('⚠️ OPENAI_API_KEY not set, skipping LLM classification');
    return { type: null, confidence: 0, llmClassified: false };
  }

  try {
    // Build context-rich prompt including excerpt
    let userPrompt = `Group: ${groupName}\nSubject: ${subject}`;
    
    if (excerpt && excerpt.trim().length > 20) {
      // Clean and truncate excerpt for prompt
      const cleanExcerpt = excerpt
        .replace(/\s+/g, ' ')
        .trim()
        .substring(0, 400);
      userPrompt += `\n\nContent excerpt:\n${cleanExcerpt}`;
    }
    
    userPrompt += '\n\nType:';

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
      return { type: null, confidence: 0, llmClassified: false };
    }

    const data = await response.json();
    const rawType = data.choices[0]?.message?.content?.trim().toLowerCase();
    
    // Validate the type
    const validTypes = ['cip', 'validator', 'featured-app', 'protocol-upgrade', 'outcome', 'other'];
    const type = validTypes.includes(rawType) ? rawType : null;
    
    // Log with excerpt indicator
    const excerptIndicator = excerpt ? ' [+excerpt]' : '';
    console.log(`🤖 LLM classified "${subject.slice(0, 50)}..."${excerptIndicator} -> ${type}`);
    
    return { 
      type, 
      confidence: type ? 0.90 : 0,  // Higher confidence when using excerpt
      llmClassified: true
    };
  } catch (error) {
    console.error('❌ LLM classification error:', error.message);
    return { type: null, confidence: 0, llmClassified: false };
  }
}

/**
 * Batch classify multiple topics with excerpts (more efficient API usage)
 * @param {Array<{subject: string, groupName: string, id: string, excerpt?: string}>} topics
 * @returns {Promise<Map<string, {type: string, confidence: number, llmClassified: boolean}>>}
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
      const result = await classifyTopic(
        topic.subject, 
        topic.groupName, 
        topic.excerpt || ''
      );
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
  
  console.log(`🤖 Batch classification complete: ${results.size} topics processed`);
  return results;
}

/**
 * Check if LLM classification is available
 */
export function isLLMAvailable() {
  return !!OPENAI_API_KEY;
}
