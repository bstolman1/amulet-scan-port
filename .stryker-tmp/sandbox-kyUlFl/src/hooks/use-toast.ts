// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import * as React from "react";
import type { ToastActionElement, ToastProps } from "@/components/ui/toast";
const TOAST_LIMIT = 1;
const TOAST_REMOVE_DELAY = 1000000;
type ToasterToast = ToastProps & {
  id: string;
  title?: React.ReactNode;
  description?: React.ReactNode;
  action?: ToastActionElement;
};
const actionTypes = {
  ADD_TOAST: "ADD_TOAST",
  UPDATE_TOAST: "UPDATE_TOAST",
  DISMISS_TOAST: "DISMISS_TOAST",
  REMOVE_TOAST: "REMOVE_TOAST"
} as const;
let count = 0;
function genId() {
  if (stryMutAct_9fa48("2912")) {
    {}
  } else {
    stryCov_9fa48("2912");
    count = stryMutAct_9fa48("2913") ? (count + 1) * Number.MAX_SAFE_INTEGER : (stryCov_9fa48("2913"), (stryMutAct_9fa48("2914") ? count - 1 : (stryCov_9fa48("2914"), count + 1)) % Number.MAX_SAFE_INTEGER);
    return count.toString();
  }
}
type ActionType = typeof actionTypes;
type Action = {
  type: ActionType["ADD_TOAST"];
  toast: ToasterToast;
} | {
  type: ActionType["UPDATE_TOAST"];
  toast: Partial<ToasterToast>;
} | {
  type: ActionType["DISMISS_TOAST"];
  toastId?: ToasterToast["id"];
} | {
  type: ActionType["REMOVE_TOAST"];
  toastId?: ToasterToast["id"];
};
interface State {
  toasts: ToasterToast[];
}
const toastTimeouts = new Map<string, ReturnType<typeof setTimeout>>();
const addToRemoveQueue = (toastId: string) => {
  if (stryMutAct_9fa48("2915")) {
    {}
  } else {
    stryCov_9fa48("2915");
    if (stryMutAct_9fa48("2917") ? false : stryMutAct_9fa48("2916") ? true : (stryCov_9fa48("2916", "2917"), toastTimeouts.has(toastId))) {
      if (stryMutAct_9fa48("2918")) {
        {}
      } else {
        stryCov_9fa48("2918");
        return;
      }
    }
    const timeout = setTimeout(() => {
      if (stryMutAct_9fa48("2919")) {
        {}
      } else {
        stryCov_9fa48("2919");
        toastTimeouts.delete(toastId);
        dispatch({
          type: "REMOVE_TOAST",
          toastId: toastId
        });
      }
    }, TOAST_REMOVE_DELAY);
    toastTimeouts.set(toastId, timeout);
  }
};
export const reducer = (state: State, action: Action): State => {
  if (stryMutAct_9fa48("2922")) {
    {}
  } else {
    stryCov_9fa48("2922");
    switch (action.type) {
      case "ADD_TOAST":
        if (stryMutAct_9fa48("2923")) {} else {
          stryCov_9fa48("2923");
          return {
            ...state,
            toasts: stryMutAct_9fa48("2926") ? [action.toast, ...state.toasts] : (stryCov_9fa48("2926"), (stryMutAct_9fa48("2927") ? [] : (stryCov_9fa48("2927"), [action.toast, ...state.toasts])).slice(0, TOAST_LIMIT))
          };
        }
      case "UPDATE_TOAST":
        if (stryMutAct_9fa48("2928")) {} else {
          stryCov_9fa48("2928");
          return {
            ...state,
            toasts: state.toasts.map(stryMutAct_9fa48("2931") ? () => undefined : (stryCov_9fa48("2931"), t => (stryMutAct_9fa48("2934") ? t.id !== action.toast.id : stryMutAct_9fa48("2933") ? false : stryMutAct_9fa48("2932") ? true : (stryCov_9fa48("2932", "2933", "2934"), t.id === action.toast.id)) ? {
              ...t,
              ...action.toast
            } : t))
          };
        }
      case "DISMISS_TOAST":
        if (stryMutAct_9fa48("2936")) {} else {
          stryCov_9fa48("2936");
          {
            if (stryMutAct_9fa48("2938")) {
              {}
            } else {
              stryCov_9fa48("2938");
              const {
                toastId
              } = action;

              // ! Side effects ! - This could be extracted into a dismissToast() action,
              // but I'll keep it here for simplicity
              if (stryMutAct_9fa48("2940") ? false : stryMutAct_9fa48("2939") ? true : (stryCov_9fa48("2939", "2940"), toastId)) {
                if (stryMutAct_9fa48("2941")) {
                  {}
                } else {
                  stryCov_9fa48("2941");
                  addToRemoveQueue(toastId);
                }
              } else {
                if (stryMutAct_9fa48("2942")) {
                  {}
                } else {
                  stryCov_9fa48("2942");
                  state.toasts.forEach(toast => {
                    if (stryMutAct_9fa48("2943")) {
                      {}
                    } else {
                      stryCov_9fa48("2943");
                      addToRemoveQueue(toast.id);
                    }
                  });
                }
              }
              return {
                ...state,
                toasts: state.toasts.map(stryMutAct_9fa48("2945") ? () => undefined : (stryCov_9fa48("2945"), t => (stryMutAct_9fa48("2948") ? t.id === toastId && toastId === undefined : stryMutAct_9fa48("2947") ? false : stryMutAct_9fa48("2946") ? true : (stryCov_9fa48("2946", "2947", "2948"), (stryMutAct_9fa48("2950") ? t.id !== toastId : stryMutAct_9fa48("2949") ? false : (stryCov_9fa48("2949", "2950"), t.id === toastId)) || (stryMutAct_9fa48("2952") ? toastId !== undefined : stryMutAct_9fa48("2951") ? false : (stryCov_9fa48("2951", "2952"), toastId === undefined)))) ? {
                  ...t,
                  open: stryMutAct_9fa48("2954") ? true : (stryCov_9fa48("2954"), false)
                } : t))
              };
            }
          }
        }
      case "REMOVE_TOAST":
        if (stryMutAct_9fa48("2955")) {} else {
          stryCov_9fa48("2955");
          if (stryMutAct_9fa48("2959") ? action.toastId !== undefined : stryMutAct_9fa48("2958") ? false : stryMutAct_9fa48("2957") ? true : (stryCov_9fa48("2957", "2958", "2959"), action.toastId === undefined)) {
            if (stryMutAct_9fa48("2960")) {
              {}
            } else {
              stryCov_9fa48("2960");
              return {
                ...state,
                toasts: stryMutAct_9fa48("2962") ? ["Stryker was here"] : (stryCov_9fa48("2962"), [])
              };
            }
          }
          return {
            ...state,
            toasts: stryMutAct_9fa48("2964") ? state.toasts : (stryCov_9fa48("2964"), state.toasts.filter(stryMutAct_9fa48("2965") ? () => undefined : (stryCov_9fa48("2965"), t => stryMutAct_9fa48("2968") ? t.id === action.toastId : stryMutAct_9fa48("2967") ? false : stryMutAct_9fa48("2966") ? true : (stryCov_9fa48("2966", "2967", "2968"), t.id !== action.toastId))))
          };
        }
    }
  }
};
const listeners: Array<(state: State) => void> = stryMutAct_9fa48("2969") ? ["Stryker was here"] : (stryCov_9fa48("2969"), []);
let memoryState: State = {
  toasts: stryMutAct_9fa48("2971") ? ["Stryker was here"] : (stryCov_9fa48("2971"), [])
};
function dispatch(action: Action) {
  if (stryMutAct_9fa48("2972")) {
    {}
  } else {
    stryCov_9fa48("2972");
    memoryState = reducer(memoryState, action);
    listeners.forEach(listener => {
      if (stryMutAct_9fa48("2973")) {
        {}
      } else {
        stryCov_9fa48("2973");
        listener(memoryState);
      }
    });
  }
}
type Toast = Omit<ToasterToast, "id">;
function toast({
  ...props
}: Toast) {
  if (stryMutAct_9fa48("2974")) {
    {}
  } else {
    stryCov_9fa48("2974");
    const id = genId();
    const update = stryMutAct_9fa48("2975") ? () => undefined : (stryCov_9fa48("2975"), (() => {
      const update = (props: ToasterToast) => dispatch({
        type: "UPDATE_TOAST",
        toast: {
          ...props,
          id
        }
      });
      return update;
    })());
    const dismiss = stryMutAct_9fa48("2979") ? () => undefined : (stryCov_9fa48("2979"), (() => {
      const dismiss = () => dispatch({
        type: "DISMISS_TOAST",
        toastId: id
      });
      return dismiss;
    })());
    dispatch({
      type: "ADD_TOAST",
      toast: {
        ...props,
        id,
        open: stryMutAct_9fa48("2985") ? false : (stryCov_9fa48("2985"), true),
        onOpenChange: open => {
          if (stryMutAct_9fa48("2986")) {
            {}
          } else {
            stryCov_9fa48("2986");
            if (stryMutAct_9fa48("2989") ? false : stryMutAct_9fa48("2988") ? true : stryMutAct_9fa48("2987") ? open : (stryCov_9fa48("2987", "2988", "2989"), !open)) dismiss();
          }
        }
      }
    });
    return {
      id: id,
      dismiss,
      update
    };
  }
}
function useToast() {
  if (stryMutAct_9fa48("2991")) {
    {}
  } else {
    stryCov_9fa48("2991");
    const [state, setState] = React.useState<State>(memoryState);
    React.useEffect(() => {
      if (stryMutAct_9fa48("2992")) {
        {}
      } else {
        stryCov_9fa48("2992");
        listeners.push(setState);
        return () => {
          if (stryMutAct_9fa48("2993")) {
            {}
          } else {
            stryCov_9fa48("2993");
            const index = listeners.indexOf(setState);
            if (stryMutAct_9fa48("2997") ? index <= -1 : stryMutAct_9fa48("2996") ? index >= -1 : stryMutAct_9fa48("2995") ? false : stryMutAct_9fa48("2994") ? true : (stryCov_9fa48("2994", "2995", "2996", "2997"), index > (stryMutAct_9fa48("2998") ? +1 : (stryCov_9fa48("2998"), -1)))) {
              if (stryMutAct_9fa48("2999")) {
                {}
              } else {
                stryCov_9fa48("2999");
                listeners.splice(index, 1);
              }
            }
          }
        };
      }
    }, stryMutAct_9fa48("3000") ? [] : (stryCov_9fa48("3000"), [state]));
    return {
      ...state,
      toast,
      dismiss: stryMutAct_9fa48("3002") ? () => undefined : (stryCov_9fa48("3002"), (toastId?: string) => dispatch({
        type: "DISMISS_TOAST",
        toastId
      }))
    };
  }
}
export { useToast, toast };