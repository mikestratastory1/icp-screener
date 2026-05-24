export async function POST(request) {
  try {
    const { prompt, useWebSearch, model, apiKey, maxTokens, systemPrompt } = await request.json();

    const resolvedKey = apiKey || process.env.ANTHROPIC_API_KEY;

    if (!resolvedKey) {
      return Response.json({ error: 'API key required. Set ANTHROPIC_API_KEY env var or provide in settings.' }, { status: 400 });
    }

    const body = {
      model: model || "claude-sonnet-4-20250514",
      max_tokens: maxTokens || 16000,
      messages: [{ role: "user", content: prompt }]
    };

    if (systemPrompt) {
      body.system = systemPrompt;
    }

    if (useWebSearch) {
      body.tools = [{ type: "web_search_20250305", name: "web_search" }];
    }

    // Retry logic with exponential backoff for rate limits
    let lastError = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      const response = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "anthropic-version": "2023-06-01",
          "x-api-key": resolvedKey
        },
        body: JSON.stringify(body)
      });

      const data = await response.json();

      // Check for rate limit or overloaded error
      if (data.error && data.error.message && (data.error.message.includes('rate limit') || data.error.message.includes('verloaded'))) {
        lastError = data.error.message;
        const waitTime = Math.pow(2, attempt + 1) * 30000; // 60s, 120s, 240s
        console.log(`${data.error.message.includes('verloaded') ? 'Overloaded' : 'Rate limited'}, waiting ${waitTime/1000}s before retry ${attempt + 1}/3`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
        continue;
      }

      if (data.error && data.error === 'Overloaded') {
        lastError = 'Overloaded';
        const waitTime = Math.pow(2, attempt + 1) * 30000;
        console.log(`Overloaded, waiting ${waitTime/1000}s before retry ${attempt + 1}/3`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
        continue;
      }

      if (data.error) {
        return Response.json({ error: data.error.message }, { status: 400 });
      }

      return Response.json(data);
    }

    // If we get here, all retries failed
    return Response.json({ error: `Rate limit exceeded after 3 retries. Last error: ${lastError}` }, { status: 429 });
  } catch (error) {
    console.error('API route error:', error);
    return Response.json({ error: error.message }, { status: 500 });
  }
}
