import { USER_AGENT } from "./planetscale-api.ts";

const DEFAULT_FEED_URL = "https://planetscale.com/blog/feed.atom";
const DEFAULT_SITEMAP_URL = "https://planetscale.com/blog/sitemap.xml";

/** Longest `snippet` kept per blog hit, matching the docs-search cap. */
const SNIPPET_MAX_LENGTH = 1500;

/** How many blog hits to fold into a docs search. */
export const BLOG_RESULT_LIMIT = 5;

const STOPWORDS = new Set([
  "a",
  "an",
  "the",
  "of",
  "for",
  "to",
  "in",
  "on",
  "and",
  "or",
  "is",
  "are",
  "was",
  "be",
  "how",
  "what",
  "why",
  "with",
  "from",
  "about",
  "blog",
  "blogs",
  "post",
  "posts",
  "article",
  "articles",
  "docs",
  "documentation",
  "planetscale",
]);

export type BlogPost = {
  slug: string;
  title: string;
  url: string;
  /** Plain-text body used for ranking; omitted for sitemap-only posts. */
  content?: string;
};

export type BlogSearchHit = {
  title: string;
  url: string;
  page: string;
  snippet?: string;
  truncated?: boolean;
};

export function tokenizeQuery(query: string): string[] {
  const raw = query
    .toLowerCase()
    .split(/[^a-z0-9]+/u)
    .filter((token) => token.length > 1);

  const filtered = raw.filter((token) => !STOPWORDS.has(token));
  return filtered.length > 0 ? filtered : raw.filter((token) => token.length > 2);
}

export function htmlToPlainText(html: string): string {
  return decodeEntities(
    html
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
  )
    .replace(/\s+/g, " ")
    .trim();
}

function decodeEntities(value: string): string {
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&#x27;/gi, "'")
    .replace(/&#(\d+);/g, (_, code: string) =>
      String.fromCharCode(Number(code))
    )
    .replace(/&#x([0-9a-f]+);/gi, (_, code: string) =>
      String.fromCharCode(parseInt(code, 16))
    );
}

function firstTag(block: string, tag: string): string | undefined {
  const match = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, "i").exec(
    block
  );
  if (!match || match[1] === undefined) {
    return undefined;
  }
  return decodeEntities(match[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")).trim();
}

function linkHref(block: string): string | undefined {
  const match = /<link\b[^>]*\bhref="([^"]+)"/i.exec(block);
  return match?.[1];
}

function slugFromBlogUrl(url: string): string | undefined {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return undefined;
  }

  if (parsed.hostname !== "planetscale.com") {
    return undefined;
  }

  const match = /^\/blog\/([^/]+)\/?$/.exec(parsed.pathname);
  if (!match || match[1] === undefined) {
    return undefined;
  }

  const slug = match[1];
  if (slug === "author" || slug === "category" || slug === "tag") {
    return undefined;
  }
  return slug;
}

export function titleFromSlug(slug: string): string {
  const words = slug.split("-").filter(Boolean);
  if (words.length === 0) {
    return slug;
  }
  const [first, ...rest] = words;
  if (first === undefined) {
    return slug;
  }
  return [first.charAt(0).toUpperCase() + first.slice(1), ...rest].join(" ");
}

function truncateSnippet(content: string): {
  snippet: string;
  truncated: boolean;
} {
  const trimmed = content.trim();
  if (trimmed.length <= SNIPPET_MAX_LENGTH) {
    return { snippet: trimmed, truncated: false };
  }
  return { snippet: `${trimmed.slice(0, SNIPPET_MAX_LENGTH)}…`, truncated: true };
}

/**
 * Parse PlanetScale's public Atom feed. Each `<entry>` is one post; recent
 * Markdown posts carry a full HTML body in `<content>`, standalone posts
 * often only have an excerpt.
 */
export function parseAtomFeed(xml: string): BlogPost[] {
  const posts: BlogPost[] = [];
  const entryRe = /<entry>([\s\S]*?)<\/entry>/gi;

  for (const match of xml.matchAll(entryRe)) {
    const block = match[1];
    if (block === undefined) {
      continue;
    }

    const url = linkHref(block);
    if (!url) {
      continue;
    }
    const slug = slugFromBlogUrl(url);
    if (!slug) {
      continue;
    }

    const title = firstTag(block, "title") || titleFromSlug(slug);
    const html = firstTag(block, "content") || firstTag(block, "summary") || "";
    const content = html.length > 0 ? htmlToPlainText(html) : undefined;

    posts.push({ slug, title, url, content });
  }

  return posts;
}

/**
 * Collect post URLs from `https://planetscale.com/blog/sitemap.xml`.
 * Author, category, and index URLs are skipped.
 */
export function parseBlogSitemap(xml: string): BlogPost[] {
  const posts: BlogPost[] = [];
  const seen = new Set<string>();
  const locRe = /<loc>\s*([^<]+?)\s*<\/loc>/gi;

  for (const match of xml.matchAll(locRe)) {
    const url = match[1]?.trim();
    if (!url) {
      continue;
    }
    const slug = slugFromBlogUrl(url);
    if (!slug || seen.has(slug)) {
      continue;
    }
    seen.add(slug);
    posts.push({
      slug,
      title: titleFromSlug(slug),
      url: `https://planetscale.com/blog/${slug}`,
    });
  }

  return posts;
}

export function mergeBlogPosts(feed: BlogPost[], sitemap: BlogPost[]): BlogPost[] {
  const bySlug = new Map<string, BlogPost>();
  for (const post of sitemap) {
    bySlug.set(post.slug, post);
  }
  // Feed entries win: they carry a real title and usually a body.
  for (const post of feed) {
    bySlug.set(post.slug, post);
  }
  return [...bySlug.values()];
}

function tokenSet(text: string): Set<string> {
  return new Set(tokenizeQuery(text));
}

export function scoreBlogPost(post: BlogPost, queryTokens: string[]): number {
  if (queryTokens.length === 0) {
    return 0;
  }

  const titleTokens = tokenSet(post.title);
  const slugTokens = tokenSet(post.slug.replaceAll("-", " "));
  const contentTokens = post.content ? tokenSet(post.content) : new Set<string>();

  let score = 0;
  let matched = 0;
  let titleHits = 0;

  for (const token of queryTokens) {
    if (titleTokens.has(token)) {
      score += 10;
      titleHits += 1;
      matched += 1;
    } else if (slugTokens.has(token)) {
      score += 6;
      matched += 1;
    } else if (contentTokens.has(token)) {
      score += 2;
      matched += 1;
    }
  }

  // AND matching keeps weakly related posts out of the docs result list.
  if (matched !== queryTokens.length) {
    return 0;
  }

  if (titleHits === queryTokens.length) {
    score += 25;
  }
  if (post.content) {
    score += 1;
  }

  return score;
}

export function rankBlogPosts(posts: BlogPost[], query: string): BlogSearchHit[] {
  const queryTokens = tokenizeQuery(query);
  if (queryTokens.length === 0) {
    return [];
  }

  return posts
    .map((post) => ({ post, score: scoreBlogPost(post, queryTokens) }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score || a.post.slug.localeCompare(b.post.slug))
    .slice(0, BLOG_RESULT_LIMIT)
    .map(({ post }) => toHit(post));
}

function toHit(post: BlogPost): BlogSearchHit {
  const hit: BlogSearchHit = {
    title: post.title,
    url: post.url,
    page: `blog/${post.slug}`,
  };

  const body = post.content;
  if (body !== undefined && body.length > 0) {
    const { snippet, truncated } = truncateSnippet(body);
    hit.snippet = snippet;
    if (truncated) {
      hit.truncated = true;
    }
  }

  return hit;
}

async function fetchText(
  url: string,
  signal: AbortSignal | undefined
): Promise<string> {
  const response = await fetch(url, {
    signal,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "application/atom+xml, application/xml, text/xml, */*",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  return response.text();
}

/**
 * Search public PlanetScale blog posts. Failures return an empty list so a
 * blog outage cannot hide a successful docs search.
 */
export async function searchPlanetScaleBlogs(
  query: string,
  options: {
    signal?: AbortSignal;
    feedUrl?: string;
    sitemapUrl?: string;
  } = {}
): Promise<BlogSearchHit[]> {
  const queryTokens = tokenizeQuery(query);
  if (queryTokens.length === 0) {
    return [];
  }

  const feedUrl = options.feedUrl ?? DEFAULT_FEED_URL;
  const sitemapUrl = options.sitemapUrl ?? DEFAULT_SITEMAP_URL;

  const [feedResult, sitemapResult] = await Promise.allSettled([
    fetchText(feedUrl, options.signal),
    fetchText(sitemapUrl, options.signal),
  ]);

  const feed =
    feedResult.status === "fulfilled" ? parseAtomFeed(feedResult.value) : [];
  const sitemap =
    sitemapResult.status === "fulfilled"
      ? parseBlogSitemap(sitemapResult.value)
      : [];

  if (feed.length === 0 && sitemap.length === 0) {
    return [];
  }

  return rankBlogPosts(mergeBlogPosts(feed, sitemap), query);
}
