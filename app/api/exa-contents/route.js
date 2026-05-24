export async function POST(request) {
  try {
    const { urls, text, highlights, summary, subpages, subpageTarget, maxAgeHours, livecrawlTimeout } = await request.json();

    const exaKey = process.env.EXA_API_KEY;
    if (!exaKey) {
      return Response.json({ error: 'EXA_API_KEY not set in environment variables.' }, { status: 400 });
    }

    if (!urls || !Array.isArray(urls) || urls.length === 0) {
      return Response.json({ error: 'urls array is required.' }, { status: 400 });
    }

    const body = { ids: urls };

    // Content extraction options
    if (text !== undefined) body.text = text;
    if (highlights !== undefined) body.highlights = highlights;
    if (summary !== undefined) body.summary = summary;
    if (subpages !== undefined) body.subpages = subpages;
    if (subpageTarget !== undefined) body.subpageTarget = subpageTarget;
    if (maxAgeHours !== undefined) body.maxAgeHours = maxAgeHours;
    if (livecrawlTimeout !== undefined) body.livecrawlTimeout = livecrawlTimeout;

    // Retry logic
    let lastError = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await fetch("https://api.exa.ai/contents", {
          method: "POST",
          headers: {
            "x-api-key": exaKey,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(30000)
        });

        const data = await response.json();

        if (!response.ok) {
          if (response.status === 429 || response.status >= 500) {
            lastError = data.error || `HTTP ${response.status}`;
            const waitTime = Math.pow(2, attempt + 1) * 2000;
            console.log(`Exa contents rate limit/error (${response.status}), waiting ${waitTime/1000}s before retry ${attempt + 1}/3`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            continue;
          }
          return Response.json({ error: data.error || `Exa contents error: ${response.status}` }, { status: response.status });
        }

        return Response.json(data);
      } catch (err) {
        lastError = err.message;
        if (err.name === 'TimeoutError') {
          const waitTime = Math.pow(2, attempt + 1) * 2000;
          console.log(`Exa contents timeout, waiting ${waitTime/1000}s before retry ${attempt + 1}/3`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }
        throw err;
      }
    }

    return Response.json({ error: `Exa contents failed after 3 retries. Last error: ${lastError}` }, { status: 429 });
  } catch (error) {
    console.error('Exa contents route error:', error);
    return Response.json({ error: error.message }, { status: 500 });
  }
}
