import { Info } from "lucide-react"
import { cn } from "@/lib/utils"

export function InfoTip({
  text,
  side = "top",
}: {
  text: string
  side?: "top" | "bottom"
}) {
  return (
    <span className="group relative inline-flex items-center">
      <Info className="h-3 w-3 cursor-help text-muted-foreground/60" />
      <span
        className={cn(
          "pointer-events-none absolute left-1/2 z-50 hidden w-max max-w-[240px] -translate-x-1/2 rounded-md bg-accent px-2 py-1 text-[10px] font-normal leading-snug tracking-tight text-accent-foreground shadow-sm group-hover:block",
          side === "top" ? "bottom-full mb-1.5" : "top-full mt-1.5",
        )}
      >
        {text}
      </span>
    </span>
  )
}
