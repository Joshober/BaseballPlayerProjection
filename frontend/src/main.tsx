import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ClerkProvider } from "@clerk/clerk-react";
import App from "./App";
import { WatchlistProvider } from "./context/WatchlistContext";
import "./index.css";

const queryClient = new QueryClient();
const clerkPub = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY || "";

function Root() {
  if (clerkPub && clerkPub.startsWith("pk_")) {
    return (
      <ClerkProvider publishableKey={clerkPub}>
        <QueryClientProvider client={queryClient}>
          <BrowserRouter>
            <WatchlistProvider>
              <App />
            </WatchlistProvider>
          </BrowserRouter>
        </QueryClientProvider>
      </ClerkProvider>
    );
  }
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <WatchlistProvider>
          <App />
        </WatchlistProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
  </React.StrictMode>,
);
