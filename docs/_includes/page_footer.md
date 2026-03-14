<!-- Auto-generated Page Footer Template -->
<!-- This template should be included at the bottom of documentation pages -->
<!-- Customize the variables based on page front-matter -->

---

## 📚 Page Information

**Who is this for:** {{ page.meta.roles | join(', ') | title }}  
**Time to complete:** {{ page.meta.time }}  

{% if page.meta.prereqs %}
**Prerequisites:**  
{% for prereq in page.meta.prereqs %}
- [{{ prereq | basename }}]({{ prereq }})
{% endfor %}
{% endif %}

{% if page.meta.next %}
**Up next:**  
{% for next_page in page.meta.next %}
- [{{ next_page | basename }}]({{ next_page }})
{% endfor %}
{% endif %}

{% if page.meta.related %}
**Related:**  
{% for related_page in page.meta.related %}
[{{ related_page | basename }}]({{ related_page }}){% if not loop.last %} • {% endif %}
{% endfor %}
{% endif %}

---

## 💡 Need Help?

- **Stuck?** Check the [Troubleshooting Guide](../troubleshooting.md)
- **Questions?** See the [FAQ](../guides/faq.md)
- **Want to discuss?** [GitHub Discussions](https://github.com/henryodibi11/Odibi/discussions)

---

**Found an issue with this page?** [Edit on GitHub]({{ page.edit_url }})
