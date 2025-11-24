import { Card } from "@/components/ui/card";

interface StatCardProps {
  title: string;
  value: string | number;
  description?: string;
}

const StatCard = ({ title, value, description }: StatCardProps) => {
  return (
    <Card className="p-6">
      <h3 className="text-sm font-medium text-muted-foreground mb-2">{title}</h3>
      <div className="text-2xl font-bold mb-1">{value}</div>
      {description && (
        <p className="text-xs text-muted-foreground">{description}</p>
      )}
    </Card>
  );
};

export default StatCard;
