import Link from "next/link";

export default function GlobalNotFound() {
  return (
    <div className="text-center mt-5">
      <h1 className="display-5 fw-bold">404 - Page Not Found</h1>
      <p className="text-secondary mt-3">
        Sorry, the page you are looking for does not exist.
      </p>
      <Link href="/" className="btn btn-dark mt-3">
        Back to Home
      </Link>
    </div>
  );
}
