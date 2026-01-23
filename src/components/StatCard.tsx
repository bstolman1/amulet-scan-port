import { LucideIcon } from "lucide-react";
import { ReactNode } from "react";
interface StatCardProps {
  title: string;
  value: string | number;
  icon: LucideIcon;
  trend?: {
    value: string;
    positive: boolean;
  };
  gradient?: boolean;
}
export const StatCard = ({ title, value, icon: Icon, trend, gradient }: StatCardProps) => {
  return (
    <div className={`glass-card p-6 transition-smooth hover:scale-105 ${gradient ? "glow-primary" : ""}`}>
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-sm font-medium text-muted-foreground mb-2">{title}</p>
          <p className="text-3xl font-bold text-foreground mb-1">{value}</p>
          {trend && trend.value}
        </div>
        <div className={`p-3 rounded-lg ${gradient ? "gradient-primary" : "bg-muted"}`}>
          <Icon className={`h-6 w-6 ${gradient ? "text-primary-foreground" : "text-foreground"}`} />
        </div>
      </div>
    </div>
  );
};
