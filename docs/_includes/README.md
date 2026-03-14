# Documentation Includes

This directory contains reusable templates that can be included in documentation pages.

## Available Templates

### page_footer.md

Auto-generated footer for documentation pages. Uses front-matter metadata to generate:
- Who the page is for (roles)
- Time to complete
- Prerequisites (with links)
- Up next (with links)
- Related pages (with links)
- Help resources

**Usage:**

Enable in mkdocs.yml:

```yaml
markdown_extensions:
  - pymdownx.snippets:
      base_path: docs/_includes
```

Then in any page:

```markdown
# Your Page Content

---

--8<-- "page_footer.md"
```

**Front-Matter Required:**

```yaml
---
title: Page Title
roles: [jr-de, sr-de]
tags: [pattern:type]
prereqs: [link1.md]
next: [link2.md, link3.md]
related: [link4.md, link5.md]
time: 20m
---
```

## Future Templates

- `video_embed.md` - Embed videos with consistent styling
- `example_banner.md` - Banner for runnable examples
- `warning_production.md` - Production warning callout
- `beta_feature.md` - Beta feature banner

## Contributing

When creating new include templates:
1. Add to this directory
2. Document in this README
3. Test rendering with mkdocs serve
4. Update at least one page to demonstrate usage
