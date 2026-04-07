// Adds a download button (SVG + PNG) to each rendered Mermaid diagram
document.addEventListener("DOMContentLoaded", function () {
  // MkDocs Material renders mermaid asynchronously; observe for new SVGs
  const observer = new MutationObserver(function () {
    document.querySelectorAll("pre.mermaid").forEach(function (pre) {
      if (pre.dataset.downloadAdded) return;
      const svg = pre.querySelector("svg");
      if (!svg) return;
      pre.dataset.downloadAdded = "true";

      // Wrapper for buttons
      var btnWrap = document.createElement("div");
      btnWrap.style.cssText =
        "display:flex;gap:6px;justify-content:flex-end;margin-top:4px;";

      // --- SVG download ---
      var svgBtn = document.createElement("button");
      svgBtn.textContent = "\u2B07 SVG";
      svgBtn.title = "Download as SVG";
      svgBtn.style.cssText =
        "font-size:0.75rem;padding:2px 8px;cursor:pointer;border:1px solid #888;border-radius:4px;background:transparent;color:inherit;";
      svgBtn.addEventListener("click", function () {
        var clone = svg.cloneNode(true);
        clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
        var blob = new Blob([clone.outerHTML], { type: "image/svg+xml" });
        var a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "diagram.svg";
        a.click();
        URL.revokeObjectURL(a.href);
      });

      // --- PNG download ---
      var pngBtn = document.createElement("button");
      pngBtn.textContent = "\u2B07 PNG";
      pngBtn.title = "Download as PNG";
      pngBtn.style.cssText =
        "font-size:0.75rem;padding:2px 8px;cursor:pointer;border:1px solid #888;border-radius:4px;background:transparent;color:inherit;";
      pngBtn.addEventListener("click", function () {
        var clone = svg.cloneNode(true);
        clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
        var svgData = new XMLSerializer().serializeToString(clone);
        var canvas = document.createElement("canvas");
        var bbox = svg.getBoundingClientRect();
        var scale = 2; // 2x for crisp exports
        canvas.width = bbox.width * scale;
        canvas.height = bbox.height * scale;
        var ctx = canvas.getContext("2d");
        var img = new Image();
        img.onload = function () {
          ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
          var a = document.createElement("a");
          a.href = canvas.toDataURL("image/png");
          a.download = "diagram.png";
          a.click();
        };
        img.src = "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent(svgData)));
      });

      btnWrap.appendChild(svgBtn);
      btnWrap.appendChild(pngBtn);
      pre.parentNode.insertBefore(btnWrap, pre.nextSibling);
    });
  });

  observer.observe(document.body, { childList: true, subtree: true });
});
