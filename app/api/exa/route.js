export async function POST(request) {
  try {
    const { query, category, numResults, contents, startPublishedDate, endPublishedDate, includeDomains, excludeDomains } = await request.json();

    const exaKey = process.env.EXA_API_KEY;
    if (!exaKey) {
      return Response.json({ error: 'EXA_API_KEY not set in environment variables.' }, { status: 400 });
    }

    if (!query) {
      return Response.json({ error: 'Query is required.' }, { status: 400 });
    }

    const body = {
      query,
      type: "auto",
      num_results: numResults || 10,
    };

    if (category) body.category = category;
    if (startPublishedDate) body.startPublishedDate = startPublishedDate;
    if (endPublishedDate) body.endPublishedDate = endPublishedDate;
    if (includeDomains) body.includeDomains = includeDomains;
    if (excludeDomains) body.excludeDomains = excludeDomains;

    // Contents configuration
    if (contents) {
      body.contents = contents;
    }

    // Retry logic with backoff
    let lastError = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await fetch("https://api.exa.ai/search", {
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
          // Rate limit or server error — retry
          if (response.status === 429 || response.status >= 500) {
            lastError = data.error || `HTTP ${response.status}`;
            const waitTime = Math.pow(2, attempt + 1) * 2000;
            console.log(`Exa rate limit/error (${response.status}), waiting ${waitTime/1000}s before retry ${attempt + 1}/3`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            continue;
          }
          return Response.json({ error: data.error || `Exa API error: ${response.status}` }, { status: response.status });
        }

        return Response.json(data);
      } catch (err) {
        lastError = err.message;
        if (err.name === 'TimeoutError') {
          const waitTime = Math.pow(2, attempt + 1) * 2000;
          console.log(`Exa timeout, waiting ${waitTime/1000}s before retry ${attempt + 1}/3`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }
        throw err;
      }
    }

    return Response.json({ error: `Exa search failed after 3 retries. Last error: ${lastError}` }, { status: 429 });
  } catch (error) {
    console.error('Exa search route error:', error);
    return Response.json({ error: error.message }, { status: 500 });
  }
}
