import assert from "node:assert/strict";
import { test } from "node:test";
import {
  htmlToPlainText,
  mergeBlogPosts,
  parseAtomFeed,
  parseBlogSitemap,
  rankBlogPosts,
  scoreBlogPost,
  titleFromSlug,
  tokenizeQuery,
  type BlogPost,
} from "./blog-search.ts";

const FEED = `
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>What is a Neki router?</title>
    <link href="https://planetscale.com/blog/what-is-a-neki-router" />
    <content type="html"><![CDATA[<p>A Neki router gives applications a Postgres connection to one database while planning queries across shards.</p>]]></content>
    <summary><![CDATA[A Neki router gives applications a Postgres connection.]]></summary>
  </entry>
  <entry>
    <title>Making 768 servers look like 1</title>
    <link href="https://planetscale.com/blog/making-768-servers-look-like-1" />
    <content type="html"><![CDATA[<p>How to make 768 distinct Postgres servers look like 1 to your applications.</p>]]></content>
  </entry>
  <entry>
    <title>Introducing Neki</title>
    <link href="https://planetscale.com/blog/introducing-neki" />
    <content type="html"><![CDATA[<p>Neki, sharded Postgres by PlanetScale, is now available in platform preview.</p>]]></content>
  </entry>
</feed>
`.trim();

const SITEMAP = `
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://planetscale.com/blog</loc></url>
  <url><loc>https://planetscale.com/blog/author/nick</loc></url>
  <url><loc>https://planetscale.com/blog/category/neki</loc></url>
  <url><loc>https://planetscale.com/blog/what-is-a-neki-router</loc></url>
  <url><loc>https://planetscale.com/blog/announcing-vitess-6</loc></url>
</urlset>
`.trim();

test("tokenizes a blog-flavored query down to topic words", () => {
  assert.deepEqual(tokenizeQuery("planetscale blog for neki"), ["neki"]);
  assert.deepEqual(tokenizeQuery("what is a neki router"), ["neki", "router"]);
});

test("strips tags and decodes entities in feed HTML", () => {
  assert.equal(
    htmlToPlainText("<p>It&#x27;s <a href=\"/x\">sharded</a> &amp; fast</p>"),
    "It's sharded & fast"
  );
});

test("parses titles, urls and plain-text bodies out of the Atom feed", () => {
  const posts = parseAtomFeed(FEED);

  assert.equal(posts.length, 3);
  assert.equal(posts[0]?.slug, "what-is-a-neki-router");
  assert.equal(posts[0]?.title, "What is a Neki router?");
  assert.equal(
    posts[0]?.url,
    "https://planetscale.com/blog/what-is-a-neki-router"
  );
  assert.match(posts[0]?.content ?? "", /planning queries across shards/);
  assert.doesNotMatch(posts[0]?.content ?? "", /<p>/);
});

test("sitemap yields post slugs and skips author, category, and index urls", () => {
  const posts = parseBlogSitemap(SITEMAP);

  assert.deepEqual(
    posts.map((post) => post.slug).sort(),
    ["announcing-vitess-6", "what-is-a-neki-router"]
  );
  assert.equal(posts.find((post) => post.slug === "announcing-vitess-6")?.title, "Announcing vitess 6");
});

test("feed metadata wins when the same slug is also in the sitemap", () => {
  const merged = mergeBlogPosts(parseAtomFeed(FEED), parseBlogSitemap(SITEMAP));
  const router = merged.find((post) => post.slug === "what-is-a-neki-router");
  const vitess = merged.find((post) => post.slug === "announcing-vitess-6");

  assert.equal(router?.title, "What is a Neki router?");
  assert.ok(router?.content);
  assert.equal(vitess?.title, "Announcing vitess 6");
  assert.equal(vitess?.content, undefined);
});

test("a router query ranks the Neki router post first", () => {
  const hits = rankBlogPosts(parseAtomFeed(FEED), "what is a neki router");

  assert.ok(hits.length > 0);
  assert.equal(hits[0]?.url, "https://planetscale.com/blog/what-is-a-neki-router");
  assert.equal(hits[0]?.page, "blog/what-is-a-neki-router");
  assert.match(hits[0]?.snippet ?? "", /Neki router/);
});

test("older sitemap-only posts are still findable by slug", () => {
  const posts = mergeBlogPosts(parseAtomFeed(FEED), parseBlogSitemap(SITEMAP));
  const hits = rankBlogPosts(posts, "announcing vitess 6");

  assert.equal(hits[0]?.url, "https://planetscale.com/blog/announcing-vitess-6");
  assert.equal(hits[0]?.snippet, undefined);
});

test("unrelated queries do not produce blog hits", () => {
  assert.equal(rankBlogPosts(parseAtomFeed(FEED), "billing invoices").length, 0);
});

test("a partial token match is not enough", () => {
  assert.equal(
    rankBlogPosts(parseAtomFeed(FEED), "neki router billing").length,
    0
  );
});

test("title-cased slugs keep the first word capitalized", () => {
  assert.equal(titleFromSlug("what-is-a-neki-router"), "What is a neki router");
});

test("a title match outranks a body-only mention", () => {
  const posts: BlogPost[] = [
    {
      slug: "other",
      title: "Unrelated title",
      url: "https://planetscale.com/blog/other",
      content: "This paragraph mentions a neki router in passing.",
    },
    {
      slug: "what-is-a-neki-router",
      title: "What is a Neki router?",
      url: "https://planetscale.com/blog/what-is-a-neki-router",
      content: "Intro.",
    },
  ];

  assert.ok(
    scoreBlogPost(posts[1]!, ["neki", "router"]) >
      scoreBlogPost(posts[0]!, ["neki", "router"])
  );
  assert.equal(
    rankBlogPosts(posts, "neki router")[0]?.page,
    "blog/what-is-a-neki-router"
  );
});
