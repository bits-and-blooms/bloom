// Syntax highlighting
if (window.hljs) { hljs.highlightAll(); }

// Mobile nav toggle
const toggle = document.getElementById('navToggle');
const links = document.querySelector('.nav-links');
if (toggle && links) {
  toggle.addEventListener('click', () => {
    const open = links.classList.toggle('open');
    toggle.classList.toggle('open', open);
    toggle.setAttribute('aria-expanded', String(open));
  });
  // close the menu after tapping a link
  links.querySelectorAll('a').forEach(a =>
    a.addEventListener('click', () => {
      links.classList.remove('open');
      toggle.classList.remove('open');
      toggle.setAttribute('aria-expanded', 'false');
    })
  );
}

// Copy-to-clipboard buttons
document.querySelectorAll('.copy').forEach(btn => {
  btn.addEventListener('click', async () => {
    const text = btn.getAttribute('data-copy') || '';
    try {
      await navigator.clipboard.writeText(text);
    } catch (e) {
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    const original = btn.textContent;
    btn.textContent = 'Copied!';
    btn.classList.add('done');
    setTimeout(() => {
      btn.textContent = original;
      btn.classList.remove('done');
    }, 1600);
  });
});

// Back-to-top button
const toTop = document.getElementById('toTop');
if (toTop) {
  const onScroll = () => toTop.classList.toggle('show', window.scrollY > 600);
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();
}
