import { NextResponse } from 'next/server';

const CRUSTDATA_API = 'https://api.crustdata.com';
const CRUSTDATA_TOKEN = process.env.CRUSTDATA_API_KEY;

export async function POST(request) {
  if (!CRUSTDATA_TOKEN) {
    return NextResponse.json({ error: 'CRUSTDATA_API_KEY not configured' }, { status: 500 });
  }

  try {
    const body = await request.json();
    const { action, ...params } = body;

    let url, payload;

    if (action === 'autocomplete') {
      url = `${CRUSTDATA_API}/screener/companydb/autocomplete`;
      payload = {
        field: params.field,
        query: params.query,
        limit: params.limit || 10
      };
    } else if (action === 'people_autocomplete') {
      url = `${CRUSTDATA_API}/screener/persondb/autocomplete`;
      payload = {
        field: params.field,
        query: params.query,
        limit: params.limit || 10
      };
    } else if (action === 'search') {
      url = `${CRUSTDATA_API}/screener/companydb/search`;
      payload = {
        filters: params.filters,
        sorts: params.sorts || [{ column: 'employee_metrics.latest_count', order: 'desc' }],
        limit: params.limit || 25,
      };
      if (params.cursor) payload.cursor = params.cursor;
    } else if (action === 'linkedin_company_search') {
      url = `${CRUSTDATA_API}/screener/screen`;
      payload = {
        filters: params.filters || [],
        page: params.page || 1,
        limit: params.limit || 25,
      };
    } else if (action === 'filters_autocomplete') {
      url = `${CRUSTDATA_API}/screener/filters/autocomplete`;
      payload = {
        filter_type: params.filter_type,
        query: params.query || '',
        limit: params.limit || 10,
      };
    } else if (action === 'people_search') {
      url = `${CRUSTDATA_API}/screener/persondb/search`;
      payload = {
        filters: params.filters,
        limit: params.limit || 50,
      };
      if (params.cursor) payload.cursor = params.cursor;
    } else if (action === 'person_enrich') {
      // GET request for person enrichment — returns business email
      // Docs: GET /screener/person/enrich?linkedin_profile_url=...&fields=business_email&enrich_realtime=true
      const qp = new URLSearchParams();
      if (params.linkedin_profile_url) qp.set('linkedin_profile_url', params.linkedin_profile_url);
      if (params.fields) qp.set('fields', params.fields);
      if (params.enrich_realtime) qp.set('enrich_realtime', 'true');

      const enrichUrl = `${CRUSTDATA_API}/screener/person/enrich?${qp.toString()}`;
      const enrichResp = await fetch(enrichUrl, {
        method: 'GET',
        headers: {
          'Authorization': `Token ${CRUSTDATA_TOKEN}`,
          'Accept': 'application/json, text/plain, */*',
          'Content-Type': 'application/json',
        },
      });

      console.log('[crustdata] person_enrich', enrichUrl, 'status:', enrichResp.status);

      // Handle 404 PE03 (profile not in DB, queued for enrichment) gracefully
      if (enrichResp.status === 404) {
        const errData = await enrichResp.text();
        console.log('[crustdata] enrich 404:', errData.substring(0, 500));
        return NextResponse.json({ error: `Profile not found. ${errData}`, status_code: 404 }, { status: 200 });
      }

      if (!enrichResp.ok) {
        const errText = await enrichResp.text();
        console.log('[crustdata] enrich error:', errText.substring(0, 500));
        return NextResponse.json({ error: `Crustdata API error: ${enrichResp.status} ${errText}` }, { status: enrichResp.status });
      }

      const enrichData = await enrichResp.json();
      return NextResponse.json(enrichData);
    } else {
      return NextResponse.json({ error: 'Invalid action.' }, { status: 400 });
    }

    const startTime = Date.now();
    console.log(`[crustdata] ${action} → POST ${url}`);
    console.log(`[crustdata] ${action} payload:`, JSON.stringify(payload).substring(0, 1000));

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 60000);

    let resp;
    try {
      resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': `Token ${CRUSTDATA_TOKEN}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
    } catch (fetchErr) {
      clearTimeout(timeout);
      const elapsed = Date.now() - startTime;
      if (fetchErr.name === 'AbortError') {
        console.log(`[crustdata] ${action} TIMEOUT after ${elapsed}ms`);
        return NextResponse.json({ error: `Crustdata API timed out after 60s. Try fewer companies or simpler filters.` }, { status: 504 });
      }
      throw fetchErr;
    }
    clearTimeout(timeout);

    const elapsed = Date.now() - startTime;
    console.log(`[crustdata] ${action} response: status=${resp.status} time=${elapsed}ms`);

    if (!resp.ok) {
      const errText = await resp.text();
      console.log(`[crustdata] ${action} error (${elapsed}ms):`, errText.substring(0, 500));
      return NextResponse.json({ error: `Crustdata API error: ${resp.status} ${errText}` }, { status: resp.status });
    }

    const parseStart = Date.now();
    const data = await resp.json();
    const parseElapsed = Date.now() - parseStart;
    console.log(`[crustdata] ${action} JSON parse: ${parseElapsed}ms, keys: [${Object.keys(data)}], profiles: ${data.profiles?.length ?? 'n/a'}, total: ${data.total_count ?? 'n/a'}`);
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
