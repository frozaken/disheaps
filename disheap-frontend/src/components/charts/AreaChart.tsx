import { AreaChart as RechartsAreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { BaseChart } from './BaseChart';

interface DataPoint {
  timestamp: string;
  [key: string]: string | number;
}

interface AreaConfig {
  key: string;
  name: string;
  color: string;
  fillOpacity?: number;
}

interface AreaChartProps {
  title: string;
  subtitle?: string;
  data: DataPoint[];
  areas: AreaConfig[];
  className?: string;
  actions?: React.ReactNode;
  loading?: boolean;
  error?: string;
  height?: number;
  showGrid?: boolean;
  showLegend?: boolean;
  xAxisLabel?: string;
  yAxisLabel?: string;
  stacked?: boolean;
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString('en-US', { 
    hour12: false, 
    hour: '2-digit', 
    minute: '2-digit' 
  });
}

function formatValue(value: number): string {
  if (value >= 1000000) {
    return (value / 1000000).toFixed(1) + 'M';
  }
  if (value >= 1000) {
    return (value / 1000).toFixed(1) + 'K';
  }
  return value.toString();
}

export function AreaChart({
  title,
  subtitle,
  data,
  areas,
  className = '',
  actions,
  loading = false,
  error,
  height = 320,
  showGrid = true,
  showLegend = true,
  xAxisLabel,
  yAxisLabel,
  stacked = false,
}: AreaChartProps) {
  return (
    <BaseChart
      title={title}
      subtitle={subtitle}
      className={className}
      actions={actions}
      loading={loading}
      error={error}
    >
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <RechartsAreaChart data={data} margin={{ top: 10, right: 30, left: 20, bottom: 20 }}>
            <defs>
              {areas.map((area) => (
                <linearGradient key={`gradient-${area.key}`} id={`gradient-${area.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={area.color} stopOpacity={area.fillOpacity || 0.8} />
                  <stop offset="95%" stopColor={area.color} stopOpacity={0.1} />
                </linearGradient>
              ))}
            </defs>
            
            {showGrid && (
              <CartesianGrid 
                strokeDasharray="3 3" 
                stroke="currentColor" 
                className="text-gray-200 dark:text-gray-700" 
              />
            )}
            
            <XAxis
              dataKey="timestamp"
              tickFormatter={formatTimestamp}
              stroke="currentColor"
              className="text-gray-600 dark:text-gray-400"
              fontSize={12}
              tickMargin={8}
              label={xAxisLabel ? { value: xAxisLabel, position: 'insideBottom', offset: -10 } : undefined}
            />
            
            <YAxis
              tickFormatter={formatValue}
              stroke="currentColor"
              className="text-gray-600 dark:text-gray-400"
              fontSize={12}
              tickMargin={8}
              label={yAxisLabel ? { value: yAxisLabel, angle: -90, position: 'insideLeft' } : undefined}
            />
            
            <Tooltip
              contentStyle={{
                backgroundColor: 'rgb(var(--color-gray-800))',
                border: '1px solid rgb(var(--color-gray-700))',
                borderRadius: '8px',
                color: 'rgb(var(--color-gray-100))',
                fontSize: '14px',
              }}
              labelFormatter={(label) => `Time: ${formatTimestamp(label as string)}`}
              formatter={(value: number, name: string) => [formatValue(value), name]}
            />
            
            {showLegend && (
              <Legend 
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="rect"
              />
            )}
            
            {areas.map((area) => (
              <Area
                key={area.key}
                type="monotone"
                dataKey={area.key}
                name={area.name}
                stackId={stacked ? 'stack' : undefined}
                stroke={area.color}
                strokeWidth={2}
                fill={`url(#gradient-${area.key})`}
                connectNulls={false}
              />
            ))}
          </RechartsAreaChart>
        </ResponsiveContainer>
      </div>
    </BaseChart>
  );
}
