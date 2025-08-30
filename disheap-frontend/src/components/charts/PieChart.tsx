import { PieChart as RechartsPieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';
import { BaseChart } from './BaseChart';

interface DataPoint {
  name: string;
  value: number;
  color?: string;
}

interface PieChartProps {
  title: string;
  subtitle?: string;
  data: DataPoint[];
  className?: string;
  actions?: React.ReactNode;
  loading?: boolean;
  error?: string;
  height?: number;
  showLegend?: boolean;
  showLabels?: boolean;
  innerRadius?: number;
  outerRadius?: number;
  colors?: string[];
}

const DEFAULT_COLORS = [
  '#3B82F6', // blue-500
  '#10B981', // emerald-500
  '#F59E0B', // amber-500
  '#EF4444', // red-500
  '#8B5CF6', // violet-500
  '#06B6D4', // cyan-500
  '#84CC16', // lime-500
  '#F97316', // orange-500
  '#EC4899', // pink-500
  '#6B7280', // gray-500
];

function formatValue(value: number): string {
  if (value >= 1000000) {
    return (value / 1000000).toFixed(1) + 'M';
  }
  if (value >= 1000) {
    return (value / 1000).toFixed(1) + 'K';
  }
  return value.toString();
}

function formatPercentage(value: number, total: number): string {
  const percentage = ((value / total) * 100).toFixed(1);
  return `${percentage}%`;
}

interface CustomLabelProps {
  cx?: number;
  cy?: number;
  midAngle?: number;
  innerRadius?: number;
  outerRadius?: number;
  percent?: number;
  value?: number;
}

function renderCustomLabel(props: CustomLabelProps) {
  const { cx, cy, midAngle, innerRadius, outerRadius, percent } = props;
  
  if (!cx || !cy || midAngle === undefined || !innerRadius || !outerRadius || !percent) return null;
  
  if (percent < 0.05) return null; // Don't show labels for slices < 5%
  
  const RADIAN = Math.PI / 180;
  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  return (
    <text 
      x={x} 
      y={y} 
      fill="white" 
      textAnchor={x > cx ? 'start' : 'end'} 
      dominantBaseline="central"
      fontSize={12}
      fontWeight="500"
    >
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
}

export function PieChart({
  title,
  subtitle,
  data,
  className = '',
  actions,
  loading = false,
  error,
  height = 320,
  showLegend = true,
  showLabels = true,
  innerRadius = 0,
  outerRadius,
  colors = DEFAULT_COLORS,
}: PieChartProps) {
  const total = data.reduce((sum, item) => sum + item.value, 0);
  
  // Assign colors to data points
  const dataWithColors = data.map((item, index) => ({
    ...item,
    color: item.color || colors[index % colors.length],
  }));

  const calculatedOuterRadius = outerRadius || Math.min(height * 0.35, 120);

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
          <RechartsPieChart margin={{ top: 10, right: 30, left: 20, bottom: 20 }}>
            <Pie
              data={dataWithColors}
              cx="50%"
              cy="50%"
              labelLine={false}
              label={showLabels ? renderCustomLabel : false}
              outerRadius={calculatedOuterRadius}
              innerRadius={innerRadius}
              fill="#8884d8"
              dataKey="value"
              stroke="none"
            >
              {dataWithColors.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            
            <Tooltip
              contentStyle={{
                backgroundColor: 'rgb(var(--color-gray-800))',
                border: '1px solid rgb(var(--color-gray-700))',
                borderRadius: '8px',
                color: 'rgb(var(--color-gray-100))',
                fontSize: '14px',
              }}
              formatter={(value: number, name: string) => [
                `${formatValue(value)} (${formatPercentage(value, total)})`,
                name
              ]}
            />
            
            {showLegend && (
              <Legend 
                wrapperStyle={{ paddingTop: '20px' }}
                iconType="circle"
                formatter={(value: string, entry: any) => (
                  <span style={{ color: entry.color || '#374151' }}>
                    {value}
                  </span>
                )}
              />
            )}
          </RechartsPieChart>
        </ResponsiveContainer>
      </div>
    </BaseChart>
  );
}
