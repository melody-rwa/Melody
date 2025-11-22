

export type HttpHeaders = HeadersInit & Record<string, string>;

export async function httpGet<T = any>(
  url: string,
  headers: HttpHeaders = {}
): Promise<T | null> {
  try {

    const res = await fetch(url, {
      method: "GET",
      headers,
    });

    if (!res.ok) {
      console.error(`httpGet failed: ${res.status} ${res.statusText}`);
      return null;
    }

    const contentType = res.headers.get("content-type");
    if (contentType && (contentType.includes("application/json") || contentType.includes('application/activity') )) {
      return (await res.json()) as T;
    } else {
      return (await res.text()) as unknown as T;
    }
  } catch (err) {
    console.error(`httpGet error: ${err}`);
    return null;
  }
}
