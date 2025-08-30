import { BarChart as RechartsBarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { BaseChart } from './BaseChart';

interface DataPoint {
  name: string;
  [key: string]: string | number;
}

interface BarConfig {
  key: string;
  name: string;
  color: string;
}

interface BarChartProps {
  title: string;
  subtitle?: string;
  data: DataPoint[];
  bars: BarConfig[];
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

function formatValue(value: number): string {
  if (value >= 1000000) {
    return (value / 1000000).toFixed(1) + 'M';
  }
  if (value >= 1000) {
    return (value / 1000).toFixed(1) + 'K';
  }
  return value.toString();
}

export function BarChart({
  title,
  subtitle,
  data,
  bars,
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
}: BarChartProps) {
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
          <RechartsBarChart data={data} margin={{ top: 10, right: 30, left: 20, bottom: 20 }}>
            {showGrid && (
              <CartesianGrid 
                strokeDasharray="3 3" 
                stroke="currentColor" 
                className="text-gray-200 dark:text-gray-700" 
              />
            )}
            
            <XAxis
              dataKey="name"
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
              formatter={(value: number, name: string) => [formatValue(value), name]}
            />
            
            {showLegend && bars.length > 1 && (
              <Legend 
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="rect"
              />
            )}
            
            {bars.map((bar) => (
              <Bar
                key={bar.key}
                dataKey={bar.key}
                name={bar.name}
                fill={bar.color}
                radius={[2, 2, 0, 0]}
                stackId={stacked ? 'stack' : undefined}
              />
            ))}
          </RechartsBarChart>
        </ResponsiveContainer>
      </div>
    </BaseChart>
  );
}
