export async function POST(request) {
  try {
    const { url } = await request.json();

    if (!url) {
      return Response.json({ error: 'URL required' }, { status: 400 });
    }

    // Try multiple URL variations
    const urlVariations = [url];
    if (url.includes('www.')) {
      urlVariations.push(url.replace('www.', ''));
    } else {
      urlVariations.push(url.replace('://', '://www.'));
    }
    // Try http if https fails
    if (url.startsWith('https://')) {
      urlVariations.push(url.replace('https://', 'http://'));
    }

    let html = null;
    let lastError = null;

    for (const tryUrl of urlVariations) {
      try {
        const response = await fetch(tryUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
          },
          redirect: 'follow',
          signal: AbortSignal.timeout(15000),
        });

        if (response.ok) {
          html = await response.text();
          break;
        }
        lastError = `${tryUrl}: HTTP ${response.status}`;
      } catch (e) {
        lastError = `${tryUrl}: ${e.message}`;
      }
    }

    if (!html) {
      return Response.json({ error: `All fetch attempts failed. Last: ${lastError}` }, { status: 400 });
    }

    // Extract meaningful text content from HTML
    // Remove script, style, noscript, svg, and other non-content tags
    let text = html
      .replace(/<script[\s\S]*?<\/script>/gi, '')
      .replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<noscript[\s\S]*?<\/noscript>/gi, '')
      .replace(/<svg[\s\S]*?<\/svg>/gi, '')
      .replace(/<!--[\s\S]*?-->/g, '');

    // Extract all links with their text and href (for finding announcement links)
    const links = [];
    const linkRegex = /<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
    let linkMatch;
    while ((linkMatch = linkRegex.exec(html)) !== null) {
      const href = linkMatch[1];
      const linkText = linkMatch[2].replace(/<[^>]+>/g, '').trim();
      if (linkText && href && !href.startsWith('#') && !href.startsWith('javascript:')) {
        links.push({ href, text: linkText });
      }
    }

    // Extract banner/announcement patterns before stripping tags
    const bannerPatterns = [];
    // Look for common banner patterns in the raw HTML
    const bannerRegexes = [
      // Top banners often in specific elements
      /(?:banner|announcement|topbar|notification|alert|promo)[^"]*"[^>]*>([\s\S]*?)<\/(?:div|section|a|span)/gi,
      // "Introducing" patterns
      /Introducing\s+[^<.]+/gi,
      // "New:" or "Now available" patterns
      /(?:New:|Now available|Just launched|Just released)[^<.]+/gi,
    ];
    for (const regex of bannerRegexes) {
      let m;
      while ((m = regex.exec(html)) !== null) {
        const cleaned = (m[1] || m[0]).replace(/<[^>]+>/g, '').trim();
        if (cleaned.length > 5 && cleaned.length < 200) {
          bannerPatterns.push(cleaned);
        }
      }
    }

    // Now strip all HTML tags for the main text
    text = text
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/\s+/g, ' ')
      .trim();

    // Truncate to ~8000 chars to keep token count reasonable
    const truncatedText = text.slice(0, 8000);

    // Find announcement-related links
    const announcementLinks = links.filter(l => {
      const t = (l.text + ' ' + l.href).toLowerCase();
      return t.includes('introduc') || t.includes('launch') || t.includes('announc') ||
             t.includes('new') || t.includes('blog') || t.includes('news') ||
             t.includes('learn more') || t.includes('read more') || t.includes('what\'s new') ||
             t.includes('changelog') || t.includes('update') || t.includes('release');
    }).slice(0, 10); // Cap at 10 links

    return Response.json({
      text: truncatedText,
      banners: bannerPatterns,
      announcementLinks,
      linkCount: links.length,
      charCount: text.length,
    });

  } catch (error) {
    console.error('Fetch page error:', error);
    return Response.json({ 
      error: error.name === 'TimeoutError' ? 'Page load timed out' : error.message 
    }, { status: 500 });
  }
}
