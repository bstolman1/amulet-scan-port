import { LucideIcon } from "lucide-react";

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
    <div 
      className={`
        canton-stat-card transition-smooth
        ${gradient ? "border-primary/20 glow-primary" : ""}
      `}
    >
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-sm font-medium text-muted-foreground mb-2">{title}</p>
          <p className={`text-3xl font-bold mb-1 ${gradient ? "canton-headline" : "text-foreground"}`}>
            {value}
          </p>
          {trend && trend.value && (
            <span className={`text-sm ${trend.positive ? "text-success" : "text-destructive"}`}>
              {trend.value}
            </span>
          )}
        </div>
        <div className={`p-3 rounded-xl ${gradient ? "gradient-primary" : "bg-muted"}`}>
          <Icon className={`h-6 w-6 ${gradient ? "text-primary-foreground" : "text-primary"}`} />
        </div>
      </div>
    </div>
  );
};
