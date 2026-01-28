import { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { useCCMarketOverview } from "@/hooks/use-kaiko-ohlcv";
import { Bell, BellOff, Plus, Trash2, TrendingUp, TrendingDown, AlertCircle } from "lucide-react";
import { toast } from "@/hooks/use-toast";

interface PriceAlert {
  id: string;
  targetPrice: number;
  type: 'above' | 'below';
  enabled: boolean;
  triggered: boolean;
  createdAt: Date;
}

interface CCPriceAlertsProps {
  enabled?: boolean;
}

const STORAGE_KEY = 'cc-price-alerts';

export function CCPriceAlerts({ enabled = true }: CCPriceAlertsProps) {
  const { data } = useCCMarketOverview(enabled);
  const [alerts, setAlerts] = useState<PriceAlert[]>([]);
  const [newPrice, setNewPrice] = useState('');
  const [newType, setNewType] = useState<'above' | 'below'>('above');
  const [notificationsEnabled, setNotificationsEnabled] = useState(false);

  const currentPrice = data?.summary?.price || null;

  // Load alerts from localStorage
  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        setAlerts(parsed.map((a: any) => ({ ...a, createdAt: new Date(a.createdAt) })));
      } catch (e) {
        console.error('Failed to load alerts:', e);
      }
    }
  }, []);

  // Save alerts to localStorage
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(alerts));
  }, [alerts]);

  // Request notification permission
  const requestNotificationPermission = useCallback(async () => {
    if (!('Notification' in window)) {
      toast({
        title: "Notifications not supported",
        description: "Your browser doesn't support notifications",
        variant: "destructive",
      });
      return;
    }

    const permission = await Notification.requestPermission();
    if (permission === 'granted') {
      setNotificationsEnabled(true);
      toast({
        title: "Notifications enabled",
        description: "You'll be notified when price alerts trigger",
      });
    } else {
      setNotificationsEnabled(false);
      toast({
        title: "Notifications blocked",
        description: "Please enable notifications in your browser settings",
        variant: "destructive",
      });
    }
  }, []);

  // Check for triggered alerts
  useEffect(() => {
    if (!currentPrice || !enabled) return;

    setAlerts(prev => prev.map(alert => {
      if (!alert.enabled || alert.triggered) return alert;

      const shouldTrigger = 
        (alert.type === 'above' && currentPrice >= alert.targetPrice) ||
        (alert.type === 'below' && currentPrice <= alert.targetPrice);

      if (shouldTrigger) {
        // Show toast
        toast({
          title: `Price Alert Triggered!`,
          description: `CC is now ${alert.type === 'above' ? 'above' : 'below'} $${alert.targetPrice.toFixed(4)} (Current: $${currentPrice.toFixed(4)})`,
        });

        // Show browser notification if enabled
        if (notificationsEnabled && Notification.permission === 'granted') {
          new Notification('CC Price Alert', {
            body: `CC is now ${alert.type === 'above' ? 'above' : 'below'} $${alert.targetPrice.toFixed(4)}`,
            icon: '/favicon.ico',
          });
        }

        return { ...alert, triggered: true };
      }

      return alert;
    }));
  }, [currentPrice, enabled, notificationsEnabled]);

  const addAlert = useCallback(() => {
    const price = parseFloat(newPrice);
    if (isNaN(price) || price <= 0) {
      toast({
        title: "Invalid price",
        description: "Please enter a valid price",
        variant: "destructive",
      });
      return;
    }

    const newAlert: PriceAlert = {
      id: Date.now().toString(),
      targetPrice: price,
      type: newType,
      enabled: true,
      triggered: false,
      createdAt: new Date(),
    };

    setAlerts(prev => [...prev, newAlert]);
    setNewPrice('');
    
    toast({
      title: "Alert created",
      description: `You'll be alerted when CC goes ${newType} $${price.toFixed(4)}`,
    });
  }, [newPrice, newType]);

  const removeAlert = useCallback((id: string) => {
    setAlerts(prev => prev.filter(a => a.id !== id));
  }, []);

  const toggleAlert = useCallback((id: string) => {
    setAlerts(prev => prev.map(a => 
      a.id === id ? { ...a, enabled: !a.enabled, triggered: false } : a
    ));
  }, []);

  const resetAlert = useCallback((id: string) => {
    setAlerts(prev => prev.map(a => 
      a.id === id ? { ...a, triggered: false } : a
    ));
  }, []);

  return (
    <Card>
      <CardHeader className="pb-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-2">
            <Bell className="h-5 w-5" />
            Price Alerts
            {currentPrice && (
              <Badge variant="secondary" className="ml-2 font-mono">
                Current: ${currentPrice.toFixed(4)}
              </Badge>
            )}
          </CardTitle>
          <Button
            variant={notificationsEnabled ? "secondary" : "outline"}
            size="sm"
            onClick={requestNotificationPermission}
            className="shrink-0"
          >
            {notificationsEnabled ? (
              <>
                <Bell className="h-4 w-4 mr-2" />
                Notifications On
              </>
            ) : (
              <>
                <BellOff className="h-4 w-4 mr-2" />
                Enable Notifications
              </>
            )}
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Add New Alert */}
        <div className="flex flex-col gap-4 p-4 rounded-lg bg-muted/50">
          <div>
            <Label htmlFor="price" className="text-xs text-muted-foreground mb-2 block">
              Target Price (USD)
            </Label>
            <Input
              id="price"
              type="number"
              step="0.0001"
              placeholder="Enter price..."
              value={newPrice}
              onChange={(e) => setNewPrice(e.target.value)}
              className="bg-background"
            />
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex rounded-md overflow-hidden border">
              <Button
                variant={newType === 'above' ? 'default' : 'ghost'}
                size="sm"
                className="rounded-none px-4"
                onClick={() => setNewType('above')}
              >
                <TrendingUp className="h-4 w-4 mr-2" />
                Above
              </Button>
              <Button
                variant={newType === 'below' ? 'default' : 'ghost'}
                size="sm"
                className="rounded-none px-4"
                onClick={() => setNewType('below')}
              >
                <TrendingDown className="h-4 w-4 mr-2" />
                Below
              </Button>
            </div>
            <Button onClick={addAlert} className="px-6">
              <Plus className="h-4 w-4 mr-2" />
              Add
            </Button>
          </div>
        </div>

        {/* Alerts List */}
        {alerts.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">
            <AlertCircle className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p className="font-medium">No price alerts set</p>
            <p className="text-sm mt-1">Add an alert to get notified when CC reaches your target price</p>
          </div>
        ) : (
          <div className="space-y-2">
            {alerts.map((alert) => (
              <div
                key={alert.id}
                className={`flex items-center gap-4 p-4 rounded-lg border ${
                  alert.triggered 
                    ? 'bg-green-500/10 border-green-500/30' 
                    : alert.enabled 
                      ? 'bg-muted/30' 
                      : 'bg-muted/10 opacity-60'
                }`}
              >
                <Switch
                  checked={alert.enabled}
                  onCheckedChange={() => toggleAlert(alert.id)}
                />
                <div className="flex-1 min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    {alert.type === 'above' ? (
                      <TrendingUp className="h-4 w-4 text-green-500 shrink-0" />
                    ) : (
                      <TrendingDown className="h-4 w-4 text-red-500 shrink-0" />
                    )}
                    <span className="font-medium">
                      {alert.type === 'above' ? 'Above' : 'Below'} ${alert.targetPrice.toFixed(4)}
                    </span>
                    {alert.triggered && (
                      <Badge variant="default" className="bg-green-500 text-white">
                        Triggered!
                      </Badge>
                    )}
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">
                    Created {alert.createdAt.toLocaleString()}
                  </p>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  {alert.triggered && (
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => resetAlert(alert.id)}
                      title="Reset alert"
                    >
                      <Bell className="h-4 w-4" />
                    </Button>
                  )}
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => removeAlert(alert.id)}
                    className="text-destructive hover:text-destructive"
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}