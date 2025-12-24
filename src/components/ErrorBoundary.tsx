import React from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";

type ErrorBoundaryProps = {
  children: React.ReactNode;
  title?: string;
};

type ErrorBoundaryState = {
  error?: Error;
};

export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = {};

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    // eslint-disable-next-line no-console
    console.error("[ErrorBoundary] Uncaught error", error, info);
  }

  private handleReload = () => {
    window.location.reload();
  };

  render() {
    if (this.state.error) {
      const title = this.props.title ?? "Something went wrong";
      return (
        <div className="min-h-[60vh] flex items-center justify-center px-6">
          <div className="w-full max-w-2xl">
            <Alert variant="destructive">
              <AlertTitle>{title}</AlertTitle>
              <AlertDescription>
                <div className="space-y-3">
                  <p className="text-sm">
                    A runtime error occurred and the page couldn’t render. See the browser console for the full stack trace.
                  </p>
                  <pre className="text-xs whitespace-pre-wrap rounded-md bg-muted/40 p-3 border border-border/60 overflow-auto max-h-56">
                    {this.state.error.message}
                  </pre>
                  <div className="flex gap-2">
                    <Button variant="destructive" onClick={this.handleReload}>
                      Reload page
                    </Button>
                  </div>
                </div>
              </AlertDescription>
            </Alert>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
