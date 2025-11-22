
import Link from "next/link";

export default function UserNotFound() {
  return (
    <div className="text-center mt-5">
      <h1 className="display-6 text-danger">User Not Found</h1>
      <p className="text-secondary mt-3">
        The user you&#39;re trying to view does not exist or has been removed.
      </p>
      <Link href="/" className="btn btn-outline-dark mt-3">
        Back to Home Page
      </Link>
    </div>
  );
}
