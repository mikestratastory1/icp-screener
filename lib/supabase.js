import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  console.warn('Supabase env vars not set — database features will be unavailable.');
}

export const supabase = supabaseUrl && supabaseAnonKey
  ? createClient(supabaseUrl, supabaseAnonKey)
  : null;

// Domain normalization: vitalize.care, www.vitalize.care, https://vitalize.care → vitalize.care
export function normalizeDomain(input) {
  try {
    let url = input.trim();
    if (!url.startsWith('http')) url = `https://${url}`;
    const hostname = new URL(url).hostname.replace(/^www\./, '');
    return hostname.toLowerCase();
  } catch {
    return input.trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '');
  }
}

// Upsert company by domain — returns company record
export async function upsertCompany(domain, name, website) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('companies')
    .upsert({ domain, name, website }, { onConflict: 'domain' })
    .select()
    .single();
  if (error) throw error;
  return data;
}

// Create a new research run for a company
export async function createResearchRun(companyId) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('research_runs')
    .insert({ company_id: companyId, status: 'pending' })
    .select()
    .single();
  if (error) throw error;
  return data;
}

// Update research run with partial data
export async function updateResearchRun(runId, updates) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('research_runs')
    .update(updates)
    .eq('id', runId)
    .select()
    .single();
  if (error) throw error;
  return data;
}

// Get all companies with their latest research run
export async function getCompaniesWithLatest() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('company_latest')
    .select('*')
    .order('updated_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

// Get a single company by domain with latest research
export async function getCompanyByDomain(domain) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('company_latest')
    .select('*')
    .eq('domain', domain)
    .single();
  if (error && error.code !== 'PGRST116') throw error; // PGRST116 = not found
  return data || null;
}

// Get all research runs for a company (history)
export async function getResearchHistory(companyId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('research_runs')
    .select('*')
    .eq('company_id', companyId)
    .order('created_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

// Update company manual score / notes
export async function updateCompany(companyId, updates) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('companies')
    .update(updates)
    .eq('id', companyId)
    .select()
    .single();
  if (error) throw error;
  return data;
}

// Delete a company and all its research runs
export async function deleteCompany(companyId) {
  if (!supabase) return;
  const { error } = await supabase
    .from('companies')
    .delete()
    .eq('id', companyId);
  if (error) throw error;
}

// Save a training example (upsert by domain + factor)
export async function saveTrainingExample(example) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('training_examples')
    .upsert(example, { onConflict: 'domain,factor' })
    .select()
    .single();
  if (error) throw error;
  return data;
}

// Get all training examples
export async function getTrainingExamples() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('training_examples')
    .select('*')
    .order('created_at', { ascending: true });
  if (error) throw error;
  return data || [];
}

// Delete a training example
export async function deleteTrainingExample(id) {
  if (!supabase) return;
  const { error } = await supabase
    .from('training_examples')
    .delete()
    .eq('id', id);
  if (error) throw error;
}

// ======= CONTACTS =======

// Upsert contact by linkedin URL — returns contact record
export async function upsertContact(contact) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('contacts')
    .upsert(contact, { onConflict: 'linkedin' })
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function updateContact(contactId, updates) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('contacts')
    .update(updates)
    .eq('id', contactId)
    .select()
    .single();
  if (error) throw error;
  return data;
}

// Bulk upsert contacts
export async function upsertContacts(contacts) {
  if (!supabase || contacts.length === 0) return [];
  const { data, error } = await supabase
    .from('contacts')
    .upsert(contacts, { onConflict: 'linkedin' })
    .select();
  if (error) throw error;
  return data || [];
}

// Get all contacts
export async function getAllContacts() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('contacts')
    .select('*, companies(id, name, domain, website)')
    .order('created_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

// Get contacts for a specific company
export async function getContactsByCompany(companyId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('contacts')
    .select('*')
    .eq('company_id', companyId)
    .order('name', { ascending: true });
  if (error) throw error;
  return data || [];
}

// Delete a contact
export async function deleteContact(id) {
  if (!supabase) return;
  const { error } = await supabase
    .from('contacts')
    .delete()
    .eq('id', id);
  if (error) throw error;
}

// ======= CAMPAIGNS =======

export async function getAllCampaigns() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('campaigns')
    .select('*')
    .order('created_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

export async function createCampaign(name = 'Untitled Campaign') {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('campaigns')
    .insert({ name })
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function updateCampaign(id, fields) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('campaigns')
    .update(fields)
    .eq('id', id)
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function deleteCampaign(id) {
  if (!supabase) return;
  const { error } = await supabase
    .from('campaigns')
    .delete()
    .eq('id', id);
  if (error) throw error;
}

// Campaign contacts
export async function getCampaignContacts(campaignId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('campaign_contacts')
    .select('*, contacts(*, companies(id, name, domain))')
    .eq('campaign_id', campaignId)
    .order('added_at', { ascending: true });
  if (error) throw error;
  return data || [];
}

export async function getAllCampaignContacts() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('campaign_contacts')
    .select('campaign_id, contact_id');
  if (error) throw error;
  return data || [];
}

export async function addContactsToCampaign(campaignId, contactIds) {
  if (!supabase || contactIds.length === 0) return [];
  const rows = contactIds.map(cid => ({ campaign_id: campaignId, contact_id: cid }));
  const { data, error } = await supabase
    .from('campaign_contacts')
    .upsert(rows, { onConflict: 'campaign_id,contact_id' })
    .select();
  if (error) throw error;
  // Update last_campaign_added_at on all contacts
  const now = new Date().toISOString();
  await supabase
    .from('contacts')
    .update({ last_campaign_added_at: now })
    .in('id', contactIds);
  return data || [];
}

export async function removeContactFromCampaign(campaignId, contactId) {
  if (!supabase) return;
  const { error } = await supabase
    .from('campaign_contacts')
    .delete()
    .eq('campaign_id', campaignId)
    .eq('contact_id', contactId);
  if (error) throw error;
}

// Campaign messages
export async function getCampaignMessages(campaignId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('campaign_messages')
    .select('*')
    .eq('campaign_id', campaignId)
    .order('step_number', { ascending: true });
  if (error) throw error;
  return data || [];
}

export async function upsertCampaignMessage(message) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('campaign_messages')
    .upsert(message, { onConflict: 'id' })
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function createCampaignMessage(campaignId, channel = 'email', stepNumber = 1) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('campaign_messages')
    .insert({ campaign_id: campaignId, channel, step_number: stepNumber, subject: '', body: '' })
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function deleteCampaignMessage(id) {
  if (!supabase) return;
  const { error } = await supabase
    .from('campaign_messages')
    .delete()
    .eq('id', id);
  if (error) throw error;
}

// ======= Saved Filters =======
export async function getSavedFilters() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('saved_filters')
    .select('*')
    .order('updated_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

export async function createSavedFilter(name, mode, filters) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('saved_filters')
    .insert({ name, mode, filters })
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function updateSavedFilter(id, updates) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('saved_filters')
    .update({ ...updates, updated_at: new Date().toISOString() })
    .eq('id', id)
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function deleteSavedFilter(id) {
  if (!supabase) return;
  const { error } = await supabase
    .from('saved_filters')
    .delete()
    .eq('id', id);
  if (error) throw error;
}

// Generated messages (per-contact LLM output)
export async function getGeneratedMessages(campaignId, contactId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('generated_messages')
    .select('*')
    .eq('campaign_id', campaignId)
    .eq('contact_id', contactId)
    .order('step_number', { ascending: true });
  if (error) throw error;
  return data || [];
}

export async function upsertGeneratedMessages(campaignId, contactId, messages) {
  if (!supabase || messages.length === 0) return [];
  // Delete existing then insert fresh
  await supabase
    .from('generated_messages')
    .delete()
    .eq('campaign_id', campaignId)
    .eq('contact_id', contactId);
  const rows = messages.map((m, i) => ({
    campaign_id: campaignId,
    contact_id: contactId,
    step_number: m.step_number || i + 1,
    channel: m.channel || 'email',
    subject: m.subject || '',
    body: m.body || '',
  }));
  const { data, error } = await supabase
    .from('generated_messages')
    .insert(rows)
    .select();
  if (error) throw error;
  return data || [];
}

export async function updateGeneratedMessage(id, updates) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('generated_messages')
    .update(updates)
    .eq('id', id)
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function getGeneratedMessageContacts(campaignId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('generated_messages')
    .select('contact_id')
    .eq('campaign_id', campaignId);
  if (error) throw error;
  return [...new Set((data || []).map(d => d.contact_id))];
}
