#!/usr/bin/env node
/**
 * Fetch Super Validator announcements from Groups.io
 * 
 * Usage: node scripts/fetch-sv-announcements.js
 * 
 * Environment variables:
 *   GROUPS_IO_API_KEY - Your Groups.io API key
 */

import 'dotenv/config';

const API_KEY = process.env.GROUPS_IO_API_KEY;
const GROUP_NAME = 'supervalidator-announce';
const BASE_URL = 'https://lists.sync.global';

if (!API_KEY) {
  console.error('Error: GROUPS_IO_API_KEY environment variable is required');
  console.error('Add it to scripts/ingest/.env or set it in your environment');
  process.exit(1);
}

async function fetchMessages(limit = 50) {
  const url = `${BASE_URL}/api/v1/getmessages?group_name=${GROUP_NAME}&limit=${limit}`;
  
  console.log(`Fetching messages from ${GROUP_NAME}...`);
  
  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API request failed: ${response.status} - ${text}`);
  }

  return response.json();
}

async function fetchTopics(limit = 50) {
  const url = `${BASE_URL}/api/v1/gettopics?group_name=${GROUP_NAME}&limit=${limit}`;
  
  console.log(`Fetching topics from ${GROUP_NAME}...`);
  
  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API request failed: ${response.status} - ${text}`);
  }

  return response.json();
}

async function main() {
  try {
    // Try fetching topics first (announcements are usually organized as topics)
    const topics = await fetchTopics(20);
    console.log('\n=== Topics ===');
    console.log(JSON.stringify(topics, null, 2));

    // Also fetch recent messages
    const messages = await fetchMessages(20);
    console.log('\n=== Messages ===');
    console.log(JSON.stringify(messages, null, 2));

  } catch (error) {
    console.error('Error:', error.message);
    
    // If the standard endpoints don't work, try alternative endpoints
    console.log('\nTrying alternative API structure...');
    
    try {
      // Groups.io has different API patterns, let's try the archive endpoint
      const archiveUrl = `${BASE_URL}/api/v1/getarchives?group_name=${GROUP_NAME}`;
      const response = await fetch(archiveUrl, {
        headers: {
          'Authorization': `Bearer ${API_KEY}`,
        },
      });
      
      if (response.ok) {
        const data = await response.json();
        console.log('Archives:', JSON.stringify(data, null, 2));
      } else {
        console.log('Archive endpoint also failed:', response.status);
      }
    } catch (e) {
      console.error('Alternative also failed:', e.message);
    }
  }
}

main();
