import { NavLink as RouterNavLink, NavLinkProps } from "react-router-dom";
import { forwardRef, useCallback } from "react";
import { cn } from "@/lib/utils";
import { usePrefetch, routePrefetchMap } from "@/hooks/use-prefetch";

interface NavLinkCompatProps extends Omit<NavLinkProps, "className"> {
  className?: string;
  activeClassName?: string;
  pendingClassName?: string;
}

const NavLink = forwardRef<HTMLAnchorElement, NavLinkCompatProps>(
  ({ className, activeClassName, pendingClassName, to, ...props }, ref) => {
    const prefetch = usePrefetch();
    
    // Prefetch data on hover for instant page loads
    const handleMouseEnter = useCallback(() => {
      const path = typeof to === "string" ? to : to.pathname || "";
      const prefetchKey = routePrefetchMap[path];
      if (prefetchKey && prefetch[prefetchKey]) {
        prefetch[prefetchKey]();
      }
    }, [to, prefetch]);

    return (
      <RouterNavLink
        ref={ref}
        to={to}
        onMouseEnter={handleMouseEnter}
        className={({ isActive, isPending }) =>
          cn(className, isActive && activeClassName, isPending && pendingClassName)
        }
        {...props}
      />
    );
  },
);

NavLink.displayName = "NavLink";

export { NavLink };
