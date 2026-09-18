// Retire previous offline worker and its cached API health responses.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', event => event.waitUntil((async () => {
  for (const key of await caches.keys()) if (key.startsWith('fuel-ai-')) await caches.delete(key);
  await self.registration.unregister();
  await self.clients.claim();
})()));
