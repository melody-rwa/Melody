export async function fetchJson<T = any>(
  url: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(url, options);

  if (!res.ok) {
    throw new Error(`Fetch ${url} failed with status: ${res.status}`);
  }

  try {
    const data: unknown = await res.json();
    if (data === null || data === undefined) {
      throw new Error(`Response data is null or undefined: ${url}`);
    }
    return data as T;
  } catch  {
    throw new Error(`Invalid JSON response from ${url}`);
  }
}
