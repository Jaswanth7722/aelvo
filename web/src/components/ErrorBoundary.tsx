import { Component, type ErrorInfo, type ReactNode } from "react";

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  message: string;
}

/**
 * Catches render errors anywhere in the tree and displays a recoverable
 * error panel instead of unmounting the whole app to a white screen.
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false, message: "" };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, message: error.message || "Unknown error" };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Surface the failure to the console for debugging.
    console.error("AELVO web UI crashed:", error, info.componentStack);
  }

  private handleReload = (): void => {
    this.setState({ hasError: false, message: "" });
    window.location.reload();
  };

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "100vh",
            padding: 24,
            background: "#0f1115",
            color: "#e5e7eb",
            fontFamily: "system-ui, sans-serif",
            textAlign: "center",
          }}
        >
          <h1 style={{ fontSize: 28, marginBottom: 12 }}>Something went wrong</h1>
          <p style={{ color: "#9ca3af", marginBottom: 8 }}>
            The AELVO dashboard hit an unexpected error while rendering.
          </p>
          <code
            style={{
              display: "block",
              maxWidth: 640,
              padding: "10px 14px",
              marginBottom: 24,
              borderRadius: 8,
              background: "#1f2430",
              color: "#f87171",
              fontSize: 13,
              overflowX: "auto",
            }}
          >
            {this.state.message}
          </code>
          <button
            onClick={this.handleReload}
            style={{
              padding: "10px 22px",
              borderRadius: 8,
              border: "none",
              background: "#3b82f6",
              color: "#fff",
              fontSize: 15,
              fontWeight: 600,
              cursor: "pointer",
            }}
          >
            Reload dashboard
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
