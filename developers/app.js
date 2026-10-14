'use strict';
const root = document.documentElement;
try { root.dataset.theme = localStorage.getItem('ades-docs-theme') || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'); } catch { root.dataset.theme = 'light'; }
document.querySelector('#theme').addEventListener('click', () => {
  root.dataset.theme = root.dataset.theme === 'dark' ? 'light' : 'dark';
  try { localStorage.setItem('ades-docs-theme', root.dataset.theme); } catch {}
});
const menu = document.querySelector('#menu');
menu.addEventListener('click', () => { const open = document.body.classList.toggle('nav-open'); menu.setAttribute('aria-expanded', String(open)); });
const links = [...document.querySelectorAll('nav a')];
links.forEach(link => link.addEventListener('click', () => { document.body.classList.remove('nav-open'); menu.setAttribute('aria-expanded', 'false'); }));
if ('IntersectionObserver' in window) {
  const observer = new IntersectionObserver(entries => {
    entries.forEach(entry => { if (entry.isIntersecting) links.forEach(link => { const active = link.hash === '#' + entry.target.id; link.classList.toggle('active', active); if (active) link.setAttribute('aria-current', 'location'); else link.removeAttribute('aria-current'); }); });
  }, {rootMargin: '-10% 0px -65% 0px'});
  document.querySelectorAll('main section').forEach(section => observer.observe(section));
}
document.querySelectorAll('[data-copy]').forEach(button => button.addEventListener('click', async () => {
  try { await navigator.clipboard.writeText(button.parentElement.nextElementSibling.textContent); button.textContent = 'Copied'; } catch { button.textContent = 'Select code'; }
  setTimeout(() => { button.textContent = 'Copy'; }, 1800);
}));
const dialog = document.querySelector('#search-dialog');
const input = document.querySelector('#search-input');
const results = document.querySelector('#search-results');
const sections = [...document.querySelectorAll('main section')].map(section => ({id: section.id, title: section.querySelector('h1,h2').textContent.replace(/\s+/g, ' '), text: section.textContent.replace(/\s+/g, ' ')}));
function search() {
  const query = input.value.toLowerCase().trim();
  const matches = sections.filter(section => !query || query.split(/\s+/).every(word => section.text.toLowerCase().includes(word)));
  results.replaceChildren();
  matches.forEach(section => { const link = document.createElement('a'); link.href = '#' + section.id; link.textContent = section.title; const excerpt = document.createElement('span'); const pos = query ? Math.max(0, section.text.toLowerCase().indexOf(query.split(/\s+/)[0]) - 35) : section.title.length; excerpt.textContent = section.text.slice(pos, pos + 130) + '…'; link.append(excerpt); link.addEventListener('click', () => dialog.close()); results.append(link); });
  if (!matches.length) { const message = document.createElement('p'); message.textContent = 'No matching sections. Try “pack”, “403” or “impact”.'; results.append(message); }
}
function openSearch() { dialog.showModal(); search(); input.focus(); }
document.querySelector('#search-open').addEventListener('click', openSearch);
document.querySelector('#search-close').addEventListener('click', () => dialog.close());
input.addEventListener('input', search);
document.addEventListener('keydown', event => { if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); if (dialog.open) dialog.close(); else openSearch(); } });
dialog.addEventListener('click', event => { if (event.target === dialog) { const rect = dialog.getBoundingClientRect(); if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) dialog.close(); } });
