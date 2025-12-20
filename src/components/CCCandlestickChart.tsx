import { useMemo, useState, useCallback, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { KaikoCandle } from "@/hooks/use-kaiko-ohlcv";
import {
  ComposedChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Brush,
} from "recharts";
import { ZoomIn, ZoomOut, Move, RotateCcw, CandlestickChart } from "lucide-react";

// CC exchanges and their available pairs
const CC_EXCHANGES = [
  { value: 'krkn', label: 'Kraken', instruments: ['cc-usd', 'cc-usdt', 'cc-usdc', 'cc-eur'] },
  { value: 'gate', label: 'Gate.io', instruments: ['cc-usdt'] },
  { value: 'kcon', label: 'KuCoin', instruments: ['cc-usdt'] },
  { value: 'mexc', label: 'MEXC', instruments: ['cc-usdt', 'cc-usdc'] },
  { value: 'bbsp', label: 'Bybit Spot', instruments: ['cc-usdt', 'cc-usdc'] },
  { value: 'hitb', label: 'HitBTC', instruments: ['cc-usdt'] },
  { value: 'cnex', label: 'CoinEx', instruments: ['cc-usdt'] },
  { value: 'binc', label: 'Binance (Perp)', instruments: ['cc-usdt'] },
  { value: 'okex', label: 'OKX (Perp)', instruments: ['cc-usdt'] },
  { value: 'gtdm', label: 'Gate.io (Perp)', instruments: ['cc-usdt'] },
  { value: 'bbit', label: 'Bybit (Perp)', instruments: ['cc-usdt'] },
  { value: 'hbdm', label: 'Huobi (Perp)', instruments: ['cc-usdt'] },
];

interface CCCandlestickChartProps {
  candles: KaikoCandle[];
  isLoading: boolean;
  exchange?: string;
  instrument?: string;
  onExchangeChange?: (exchange: string, instrument: string) => void;
}

interface CandleData {
  timestamp: number;
  time: string;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  isUp: boolean;
  bodyBottom: number;
  bodyHeight: number;
  wickLow: number;
  wickHigh: number;
}

export function CCCandlestickChart({ candles, isLoading, exchange, instrument, onExchangeChange }: CCCandlestickChartProps) {
  const [zoomLevel, setZoomLevel] = useState(1);
  const [brushStartIndex, setBrushStartIndex] = useState<number | undefined>(undefined);
  const [brushEndIndex, setBrushEndIndex] = useState<number | undefined>(undefined);
  const [selectedExchange, setSelectedExchange] = useState(exchange || 'krkn');
  const [selectedInstrument, setSelectedInstrument] = useState(instrument || 'cc-usd');

  // Get available instruments for selected exchange
  const availableInstruments = useMemo(() => {
    const ex = CC_EXCHANGES.find(e => e.value === selectedExchange);
    return ex?.instruments || ['cc-usd'];
  }, [selectedExchange]);

  // Sync with parent props when they change
  useEffect(() => {
    if (exchange && exchange !== selectedExchange) {
      setSelectedExchange(exchange);
      const ex = CC_EXCHANGES.find(e => e.value === exchange);
      const validInstrument = ex?.instruments.includes(instrument || '') ? instrument : ex?.instruments[0];
      setSelectedInstrument(validInstrument || 'cc-usd');
    }
  }, [exchange, instrument]);

  const handleExchangeSelect = useCallback((value: string) => {
    setSelectedExchange(value);
    const ex = CC_EXCHANGES.find(e => e.value === value);
    const defaultInstrument = ex?.instruments[0] || 'cc-usdt';
    setSelectedInstrument(defaultInstrument);
    onExchangeChange?.(value, defaultInstrument);
  }, [onExchangeChange]);

  const handleInstrumentSelect = useCallback((value: string) => {
    setSelectedInstrument(value);
    onExchangeChange?.(selectedExchange, value);
  }, [selectedExchange, onExchangeChange]);

  const chartData = useMemo((): CandleData[] => {
    if (!candles.length) return [];
    
    return [...candles].reverse().map((candle) => {
      const open = candle.open ? parseFloat(candle.open) : 0;
      const close = candle.close ? parseFloat(candle.close) : 0;
      const high = candle.high ? parseFloat(candle.high) : 0;
      const low = candle.low ? parseFloat(candle.low) : 0;
      const isUp = close >= open;

      return {
        timestamp: candle.timestamp,
        time: new Date(candle.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        date: new Date(candle.timestamp).toLocaleDateString(),
        open,
        high,
        low,
        close,
        volume: parseFloat(candle.volume || '0'),
        isUp,
        bodyBottom: Math.min(open, close),
        bodyHeight: Math.abs(close - open),
        wickLow: low,
        wickHigh: high,
      };
    });
  }, [candles]);

  const { minPrice, maxPrice, avgPrice } = useMemo(() => {
    if (!chartData.length) return { minPrice: 0, maxPrice: 0, avgPrice: 0 };
    const lows = chartData.map(d => d.low).filter(v => v > 0);
    const highs = chartData.map(d => d.high).filter(v => v > 0);
    const min = Math.min(...lows) * 0.995;
    const max = Math.max(...highs) * 1.005;
    const avg = chartData.reduce((sum, d) => sum + d.close, 0) / chartData.length;
    return { minPrice: min, maxPrice: max, avgPrice: avg };
  }, [chartData]);

  const handleZoomIn = useCallback(() => {
    setZoomLevel(prev => Math.min(prev + 0.5, 3));
  }, []);

  const handleZoomOut = useCallback(() => {
    setZoomLevel(prev => Math.max(prev - 0.5, 0.5));
  }, []);

  const handleReset = useCallback(() => {
    setZoomLevel(1);
    setBrushStartIndex(undefined);
    setBrushEndIndex(undefined);
  }, []);

  const handleBrushChange = useCallback((domain: { startIndex?: number; endIndex?: number } | null) => {
    if (domain) {
      setBrushStartIndex(domain.startIndex);
      setBrushEndIndex(domain.endIndex);
    }
  }, []);

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <CandlestickChart className="h-5 w-5" />
            Candlestick Chart
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-[400px] w-full" />
        </CardContent>
      </Card>
    );
  }

  // Custom candlestick shape renderer
  const CandlestickShape = (props: any) => {
    const { x, y, width, payload } = props;
    if (!payload) return null;
    
    const { open, high, low, close, isUp } = payload;
    const candleWidth = Math.max(width * 0.6, 4);
    const wickWidth = 1;
    
    const color = isUp ? 'hsl(var(--chart-2))' : 'hsl(var(--destructive))';
    
    // Calculate positions relative to the price scale
    const priceRange = maxPrice - minPrice;
    const chartHeight = 350; // Approximate chart height
    
    const toY = (price: number) => {
      return ((maxPrice - price) / priceRange) * chartHeight;
    };
    
    const bodyTop = toY(Math.max(open, close));
    const bodyBottom = toY(Math.min(open, close));
    const bodyHeight = Math.max(bodyBottom - bodyTop, 1);
    
    const wickTop = toY(high);
    const wickBottom = toY(low);
    
    const centerX = x + width / 2;
    
    return (
      <g>
        {/* Wick */}
        <line
          x1={centerX}
          y1={wickTop}
          x2={centerX}
          y2={wickBottom}
          stroke={color}
          strokeWidth={wickWidth}
        />
        {/* Body */}
        <rect
          x={centerX - candleWidth / 2}
          y={bodyTop}
          width={candleWidth}
          height={bodyHeight}
          fill={isUp ? color : color}
          stroke={color}
          strokeWidth={1}
        />
      </g>
    );
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <CardTitle className="flex items-center gap-2">
            <CandlestickChart className="h-5 w-5" />
            Candlestick Chart
          </CardTitle>
          <div className="flex flex-wrap items-center gap-2">
            <Select value={selectedExchange} onValueChange={handleExchangeSelect}>
              <SelectTrigger className="w-[140px] h-8 text-sm">
                <SelectValue placeholder="Exchange" />
              </SelectTrigger>
              <SelectContent className="z-50 max-h-[300px] bg-popover">
                {CC_EXCHANGES.map((ex) => (
                  <SelectItem key={ex.value} value={ex.value}>
                    {ex.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedInstrument} onValueChange={handleInstrumentSelect}>
              <SelectTrigger className="w-[110px] h-8 text-sm">
                <SelectValue placeholder="Pair" />
              </SelectTrigger>
              <SelectContent className="z-50 bg-popover">
                {availableInstruments.map((inst) => (
                  <SelectItem key={inst} value={inst}>
                    {inst.toUpperCase()}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <div className="flex items-center gap-1 ml-2">
              <Button variant="outline" size="icon" className="h-8 w-8" onClick={handleZoomIn} title="Zoom In">
                <ZoomIn className="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" className="h-8 w-8" onClick={handleZoomOut} title="Zoom Out">
                <ZoomOut className="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" className="h-8 w-8" onClick={handleReset} title="Reset">
                <RotateCcw className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {chartData.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center text-muted-foreground">
            No chart data available
          </div>
        ) : (
          <div style={{ width: '100%', height: 480 * zoomLevel, minHeight: 480, maxHeight: 980 }}>
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart 
                data={chartData} 
                margin={{ top: 20, right: 30, left: 0, bottom: 120 }}
              >
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="time" 
                  tick={{ fontSize: 10 }}
                  className="text-muted-foreground"
                  tickLine={false}
                  axisLine={false}
                  interval="preserveStartEnd"
                  height={50}
                  tickMargin={12}
                />
                <YAxis 
                  domain={[minPrice, maxPrice]}
                  tick={{ fontSize: 10 }}
                  className="text-muted-foreground"
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(value) => `$${value.toFixed(2)}`}
                  width={65}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(var(--background))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                    padding: '12px',
                  }}
                  labelStyle={{ color: 'hsl(var(--foreground))', fontWeight: 'bold' }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.[0]?.payload) return null;
                    const p = payload[0].payload;
                    const color = p.isUp ? 'hsl(var(--chart-2))' : 'hsl(var(--destructive))';
                    return (
                      <div className="bg-background border border-border rounded-lg p-3 shadow-lg">
                        <div className="font-semibold text-foreground mb-2">{p.date} {p.time}</div>
                        <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                          <span className="text-muted-foreground">Open:</span>
                          <span className="text-foreground">${p.open.toFixed(4)}</span>
                          <span className="text-muted-foreground">High:</span>
                          <span className="text-foreground">${p.high.toFixed(4)}</span>
                          <span className="text-muted-foreground">Low:</span>
                          <span className="text-foreground">${p.low.toFixed(4)}</span>
                          <span className="text-muted-foreground">Close:</span>
                          <span style={{ color }} className="font-medium">${p.close.toFixed(4)}</span>
                        </div>
                      </div>
                    );
                  }}
                />
                <ReferenceLine 
                  y={avgPrice} 
                  stroke="hsl(var(--muted-foreground))" 
                  strokeDasharray="5 5"
                  label={{ 
                    value: `Avg: $${avgPrice.toFixed(2)}`, 
                    position: 'right',
                    fill: 'hsl(var(--muted-foreground))',
                    fontSize: 10,
                  }}
                />
                {/* Render candles as bars with custom coloring */}
                <Bar
                  dataKey="high"
                  fill="transparent"
                  stroke="transparent"
                  shape={<CandlestickShape />}
                />
                <Brush
                  dataKey="time"
                  height={28}
                  stroke="hsl(var(--primary))"
                  fill="hsl(var(--muted))"
                  travellerWidth={10}
                  startIndex={brushStartIndex}
                  endIndex={brushEndIndex}
                  onChange={handleBrushChange}
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}
        <div className="flex items-center justify-center gap-4 mt-4 text-xs text-muted-foreground">
          <div className="flex items-center gap-2">
            <Move className="h-3 w-3" />
            <span>Drag brush below chart to pan</span>
          </div>
          <div className="flex items-center gap-2">
            <ZoomIn className="h-3 w-3" />
            <span>Use zoom buttons to resize</span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}