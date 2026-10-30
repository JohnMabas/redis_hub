// A pretend database. Each call takes half a second so you can SEE the cache working.
const products = new Map([
  ['1', { id: 1, name: 'Maize (50kg)', price: 32000 }],
  ['2', { id: 2, name: 'Poultry feed (25kg)', price: 18500 }],
  ['3', { id: 3, name: 'Cassava stems (bundle)', price: 4500 }],
]);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function findProduct(id) {
  console.log(`[db] querying product ${id} ...`);
  await sleep(500);
  return products.get(String(id)) ?? null;
}

export async function updateProduct(id, changes) {
  await sleep(200);
  const existing = products.get(String(id));
  if (!existing) return null;

  const updated = { ...existing, ...changes, id: existing.id };
  products.set(String(id), updated);
  return updated;
}
