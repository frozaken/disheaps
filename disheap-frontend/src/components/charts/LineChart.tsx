import { LineChart as RechartsLineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { BaseChart } from './BaseChart';

interface DataPoint {
  timestamp: string;
  [key: string]: string | number;
}

interface LineConfig {
  key: string;
  name: string;
  color: string;
  strokeWidth?: number;
}

interface LineChartProps {
  title: string;
  subtitle?: string;
  data: DataPoint[];
  lines: LineConfig[];
  className?: string;
  actions?: React.ReactNode;
  loading?: boolean;
  error?: string;
  height?: number;
  showGrid?: boolean;
  showLegend?: boolean;
  xAxisLabel?: string;
  yAxisLabel?: string;
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

export function LineChart({
  title,
  subtitle,
  data,
  lines,
  className = '',
  actions,
  loading = false,
  error,
  height = 320,
  showGrid = true,
  showLegend = true,
  xAxisLabel,
  yAxisLabel,
}: LineChartProps) {
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
          <RechartsLineChart data={data} margin={{ top: 10, right: 30, left: 20, bottom: 20 }}>
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
                iconType="line"
              />
            )}
            
            {lines.map((line) => (
              <Line
                key={line.key}
                type="monotone"
                dataKey={line.key}
                name={line.name}
                stroke={line.color}
                strokeWidth={line.strokeWidth || 2}
                dot={{ r: 4 }}
                activeDot={{ r: 6 }}
                connectNulls={false}
              />
            ))}
          </RechartsLineChart>
        </ResponsiveContainer>
      </div>
    </BaseChart>
  );
}
