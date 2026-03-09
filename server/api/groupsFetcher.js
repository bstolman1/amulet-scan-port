/**
 * groupsFetcher.js - stub
 */

export async function getSubscribedGroups(signal) {
  return {};
}

export async function fetchGroupTopics(groupId, groupName, signal) {
  return [];
}

export function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
