import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from 'recharts';
import type { AnalysisResult } from '../types';

const COLORS = ['#6366f1', '#22d3ee', '#f59e0b', '#10b981', '#f43f5e', '#a78bfa', '#fb923c', '#34d399'];

interface Props {
  data: Record<string, unknown>[];
  analysis: AnalysisResult;
  columns: string[];
}

export function DataChart({ data, analysis, columns }: Props) {
  const { chartRecommendation, chartXCol, chartYCol } = analysis;

  if (!chartRecommendation || !chartXCol || !chartYCol) return null;
  if (!columns.includes(chartXCol) || !columns.includes(chartYCol)) return null;
  if (data.length === 0) return null;

  const xKey = chartXCol;
  const yKey = chartYCol;

  if (chartRecommendation === 'bar') {
    return (
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey={xKey} tick={{ fill: '#94a3b8', fontSize: 11 }} angle={-30} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0' }} />
          <Bar dataKey={yKey} fill={COLORS[0]} />
        </BarChart>
      </ResponsiveContainer>
    );
  }

  if (chartRecommendation === 'line') {
    return (
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey={xKey} tick={{ fill: '#94a3b8', fontSize: 11 }} angle={-30} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0' }} />
          <Line type="monotone" dataKey={yKey} stroke={COLORS[0]} dot={false} strokeWidth={2} />
        </LineChart>
      </ResponsiveContainer>
    );
  }

  if (chartRecommendation === 'pie') {
    const sliced = data.slice(0, 8);
    return (
      <ResponsiveContainer width="100%" height={320}>
        <PieChart>
          <Pie data={sliced} dataKey={yKey} nameKey={xKey} cx="50%" cy="50%" outerRadius={110} label>
            {sliced.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
          </Pie>
          <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0' }} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    );
  }

  if (chartRecommendation === 'scatter') {
    return (
      <ResponsiveContainer width="100%" height={320}>
        <ScatterChart margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey={xKey} name={xKey} tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <YAxis dataKey={yKey} name={yKey} tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0' }} cursor={{ strokeDasharray: '3 3' }} />
          <Scatter data={data} fill={COLORS[0]} />
        </ScatterChart>
      </ResponsiveContainer>
    );
  }

  return null;
}
