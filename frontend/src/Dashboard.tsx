import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
)

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface Item {
  id: number
  type: string
  title: string
}

const STORAGE_KEY = 'api_key'

export function Dashboard() {
  const [lab, setLab] = useState('lab-01')
  const [labs, setLabs] = useState<Item[]>([])
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRate[]>([])
  const [loading, setLoading] = useState(true)

  const token = localStorage.getItem(STORAGE_KEY)

  useEffect(() => {
    if (!token) return

    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => res.json())
      .then((data: Item[]) => {
        const filteredLabs = data.filter((item) => item.type === 'lab')
        setLabs(filteredLabs)
        if (filteredLabs.length > 0 && !filteredLabs.find(l => l.title.toLowerCase().includes(lab.replace('-', ' ')))) {
            // Find a good default or use the first one
            const firstLabId = filteredLabs[0].title.split(' ')[1]?.toLowerCase()
            if (firstLabId) {
                // setLab(`lab-${firstLabId}`)
            }
        }
      })
      .catch(console.error)
  }, [token])

  useEffect(() => {
    if (!token || !lab) return

    setLoading(true)
    const headers = { Authorization: `Bearer ${token}` }

    Promise.all([
      fetch(`/analytics/scores?lab=${lab}`, { headers }).then((res) => res.json()),
      fetch(`/analytics/timeline?lab=${lab}`, { headers }).then((res) => res.json()),
      fetch(`/analytics/pass-rates?lab=${lab}`, { headers }).then((res) => res.json()),
    ])
      .then(([scoresData, timelineData, passRatesData]) => {
        setScores(scoresData)
        setTimeline(timelineData)
        setPassRates(passRatesData)
        setLoading(false)
      })
      .catch((err) => {
        console.error(err)
        setLoading(false)
      })
  }, [token, lab])

  const scoreChartData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: scores.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.5)',
      },
    ],
  }

  const timelineChartData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((t) => t.submissions),
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1,
      },
    ],
  }

  if (loading) return <p>Loading Dashboard...</p>

  return (
    <div className="dashboard">
      <div className="controls">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={lab}
          onChange={(e) => setLab(e.target.value)}
        >
          <option value="lab-01">Lab 01</option>
          <option value="lab-02">Lab 02</option>
          <option value="lab-03">Lab 03</option>
          <option value="lab-04">Lab 04</option>
          <option value="lab-05">Lab 05</option>
        </select>
      </div>

      <div className="charts-grid">
        <div className="chart-container">
          <h3>Score Distribution</h3>
          <Bar data={scoreChartData} />
        </div>
        <div className="chart-container">
          <h3>Submissions Timeline</h3>
          <Line data={timelineChartData} />
        </div>
      </div>

      <div className="table-container">
        <h3>Pass Rates per Task</h3>
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Avg Score</th>
              <th>Attempts</th>
            </tr>
          </thead>
          <tbody>
            {passRates.map((pr, idx) => (
              <tr key={idx}>
                <td>{pr.task}</td>
                <td>{pr.avg_score}%</td>
                <td>{pr.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
