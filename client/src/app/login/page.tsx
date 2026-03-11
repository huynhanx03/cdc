"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const router = useRouter();

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();
    // Simple dummy login for demonstration
    if (username === "admin" && password === "admin") {
      localStorage.setItem("cdc_token", "admin_session");
      router.push("/");
    } else {
      setError("Invalid username or password");
    }
  };

  return (
    <div className="auth-container">
      <div className="glass-panel auth-box">
        <h1 className="title">CDC Platform</h1>
        <p className="subtitle">Sign in to track topics and partitions</p>

        <form onSubmit={handleLogin}>
          <div className="input-group">
            <label htmlFor="username">Username</label>
            <input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="admin"
              required
            />
          </div>

          <div className="input-group">
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="admin"
              required
            />
          </div>

          {error && <p style={{ color: "var(--error)", fontSize: "0.85rem", marginBottom: "16px" }}>{error}</p>}

          <button type="submit" className="btn">Sign In</button>
        </form>
      </div>
    </div>
  );
}
