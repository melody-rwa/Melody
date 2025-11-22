"use client";
import type React from "react";


export function stopEvent(e: React.SyntheticEvent | Event): void {

  if ("preventDefault" in e) e.preventDefault();
  if ("stopPropagation" in e) e.stopPropagation();

  const nativeEvent = "nativeEvent" in e ? (e as any).nativeEvent : e;
  if (nativeEvent) {
    if (typeof nativeEvent.stopImmediatePropagation === "function") {
      nativeEvent.stopImmediatePropagation();
    }
    if (typeof nativeEvent.stopPropagation === "function") {
      nativeEvent.stopPropagation();
    }
  }
}
