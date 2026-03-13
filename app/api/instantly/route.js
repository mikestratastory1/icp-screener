import { NextResponse } from 'next/server';

const INSTANTLY_API = 'https://api.instantly.ai/api/v2';
const INSTANTLY_TOKEN = process.env.INSTANTLY_API_KEY;

export async function POST(request) {
  if (!INSTANTLY_TOKEN) {
    return NextResponse.json({ error: 'INSTANTLY_API_KEY not configured. Add it to your .env.local file.' }, { status: 500 });
  }

  try {
    const body = await request.json();
    const { action, ...params } = body;

    if (action === 'add_leads') {
      // Add leads in bulk to an Instantly campaign
      // params: { campaign_id, leads: [{ email, first_name, last_name, company_name, ...custom_variables }] }
      const { campaign_id, leads } = params;

      if (!campaign_id) {
        return NextResponse.json({ error: 'campaign_id is required' }, { status: 400 });
      }
      if (!leads || leads.length === 0) {
        return NextResponse.json({ error: 'No leads provided' }, { status: 400 });
      }
      if (leads.length > 1000) {
        return NextResponse.json({ error: 'Maximum 1000 leads per request' }, { status: 400 });
      }

      const payload = {
        campaign_id,
        leads,
        skip_if_in_workspace: true,
        verify_leads_on_import: false,
      };

      console.log(`[instantly] Adding ${leads.length} leads to campaign ${campaign_id}`);

      const resp = await fetch(`${INSTANTLY_API}/leads/add`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${INSTANTLY_TOKEN}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const data = await resp.json();

      if (!resp.ok) {
        console.error('[instantly] Error:', resp.status, JSON.stringify(data));
        return NextResponse.json({ error: data.message || data.error || `Instantly API error: ${resp.status}` }, { status: resp.status });
      }

      console.log('[instantly] Success:', JSON.stringify(data));
      return NextResponse.json(data);

    } else if (action === 'list_campaigns') {
      // List Instantly campaigns so user can pick one
      const resp = await fetch(`${INSTANTLY_API}/campaigns?limit=50&status=1`, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${INSTANTLY_TOKEN}`,
          'Content-Type': 'application/json',
        },
      });

      const data = await resp.json();

      if (!resp.ok) {
        console.error('[instantly] list_campaigns error:', resp.status, JSON.stringify(data));
        return NextResponse.json({ error: data.message || data.error || `Instantly API error: ${resp.status}` }, { status: resp.status });
      }

      return NextResponse.json(data);

    } else {
      return NextResponse.json({ error: `Unknown action: ${action}` }, { status: 400 });
    }

  } catch (err) {
    console.error('[instantly] Internal error:', err);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
