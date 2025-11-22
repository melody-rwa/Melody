"use client";
import { useEffect, useState, RefObject } from "react";

type Opts = {
  debug?: boolean;
  waitFrames?: number; 
  matchQuery?: string;
};

export function useSidebarVisibility(
  leftRef: RefObject<HTMLElement | null>,
  rightRef: RefObject<HTMLElement | null>,
  parentRef?: RefObject<HTMLElement | null>,
  isShowBtn?: boolean,
  opts: Opts = {}
) {
  const { debug = false, waitFrames = 30, matchQuery } = opts;
  const [leftHidden, setLeftHidden] = useState<boolean>(false);
  const [rightHidden, setRightHidden] = useState<boolean>(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    let mounted = true;
    let rafId = 0;
    let frame = 0;

    const computeHidden = (el?: HTMLElement | null) => {
      if (!el) return true;
      try {
        const style = window.getComputedStyle(el);
        const displayNone = style.display === "none";
        const visibilityHidden = style.visibility === "hidden";
        const opacityZero = parseFloat(style.opacity || "1") === 0;
        const rectsZero = el.getClientRects().length === 0;
        const bb = el.getBoundingClientRect();
        const sizeZero = bb.width === 0 && bb.height === 0;
        const offsetParentNull = (el as any).offsetParent === null;
        const position = style.position || "";
        const offsetParentImpliesHidden = offsetParentNull && !position.includes("fixed");
        const result = displayNone || visibilityHidden || opacityZero || rectsZero || sizeZero || offsetParentImpliesHidden;
        if (debug) console.info("[useSidebarVisibility] check", {
          el,
          display: style.display,
          visibility: style.visibility,
          opacity: style.opacity,
          clientRects: el.getClientRects().length,
          bb,
          offsetParentNull,
          position,
          result
        });
        return !!result;
      } catch (e) {
        if (debug) console.warn("[useSidebarVisibility] computeHidden error", e);
        return true;
      }
    };

    const recomputeAll = () => {
      const l = computeHidden(leftRef.current);
      const r = computeHidden(rightRef.current);
      if (debug) console.info("[useSidebarVisibility] recompute", { left: l, right: r });
      setLeftHidden(l);
      setRightHidden(r);
    };

    const waitForRefs = () => {
      if (!mounted) return;
      const have = leftRef.current || rightRef.current || parentRef?.current;
      if (have || frame >= waitFrames) {
        recomputeAll();
        initObservers();
      } else {
        frame++;
        rafId = requestAnimationFrame(waitForRefs);
      }
    };

    // observer placeholders
    let roLeft: ResizeObserver | null = null;
    let roRight: ResizeObserver | null = null;
    let io: IntersectionObserver | null = null;
    let mo: MutationObserver | null = null;
    let mq: MediaQueryList | null = null;
    let mqListener: ((e?: any) => void) | null = null;
    const transitionTargets: HTMLElement[] = [];

    function initObservers() {
      if (!mounted) return;
      // window resize
      window.addEventListener("resize", recomputeAll);

         if (matchQuery) {
        try {
          mq = window.matchMedia(matchQuery);
          mqListener = () => recomputeAll();
          mqListener();
          if (typeof mq.addEventListener === "function") mq.addEventListener("change", mqListener);
          else (mq as any).addListener?.(mqListener);
        } catch (e) {
          if (debug) console.warn("[useSidebarVisibility] matchMedia failed", e);
        }
      }

      // ResizeObserver
      try {
        if (leftRef.current) {
          roLeft = new ResizeObserver(recomputeAll);
          roLeft.observe(leftRef.current);
        }
        if (rightRef.current) {
          roRight = new ResizeObserver(recomputeAll);
          roRight.observe(rightRef.current);
        }
      } catch (e) {
        if (debug) console.warn("[useSidebarVisibility] ResizeObserver init failed", e);
      }

      try {
        io = new IntersectionObserver(() => recomputeAll(), { threshold: 0 });
        if (leftRef.current) io.observe(leftRef.current);
        if (rightRef.current) io.observe(rightRef.current);
      } catch (e) {
        if (debug) console.warn("[useSidebarVisibility] IntersectionObserver init failed", e);
      }

      try {
        const target = parentRef?.current ?? document.body;
        mo = new MutationObserver(() => recomputeAll());
        mo.observe(target as Node, { attributes: true, attributeFilter: ["class", "style"], childList: true, subtree: true });
      } catch (e) {
        if (debug) console.warn("[useSidebarVisibility] MutationObserver init failed", e);
      }

      [parentRef?.current, leftRef.current, rightRef.current].forEach((el) => {
        if (el) {
          const node = el;
          const onTransition = () => recomputeAll();
          node.addEventListener("transitionend", onTransition);
          transitionTargets.push(node);
        }
      });
    }

    waitForRefs();

    return () => {
      mounted = false;
      cancelAnimationFrame(rafId);
      window.removeEventListener("resize", recomputeAll);
      if (roLeft) roLeft.disconnect();
      if (roRight) roRight.disconnect();
      if (io) io.disconnect();
      if (mo) mo.disconnect();
      if (mq && mqListener) {
        try {
          if (typeof mq.removeEventListener === "function") mq.removeEventListener("change", mqListener);
          else (mq as any).removeListener?.(mqListener);
        } catch  { /* ignore */ }
      }
      transitionTargets.forEach((node) => node.removeEventListener("transitionend", recomputeAll));
    };
    // NOTE: leftRef/rightRef/parentRef are stable refs usually - we depend on isShowBtn and opts to re-run
  }, [isShowBtn, matchQuery, debug, leftRef, rightRef, parentRef, waitFrames]);

  return { leftHidden, rightHidden };
}
