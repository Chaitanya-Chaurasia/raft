import { Info } from "lucide-react"

export function InfoTip({ text }: { text: string }) {
  return (
    <span className="group relative inline-flex items-center">
      <Info className="h-3 w-3 cursor-help text-muted-foreground/60" />
      <span className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-1.5 hidden w-max max-w-[240px] -translate-x-1/2 rounded-md bg-accent px-2 py-1 text-[10px] font-normal leading-snug tracking-tight text-accent-foreground shadow-sm group-hover:block">
        {text}
      </span>
    </span>
  )
}
