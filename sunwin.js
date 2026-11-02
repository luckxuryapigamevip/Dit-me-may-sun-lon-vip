import fastify from "fastify";
import cors from "@fastify/cors";
import WebSocket from "ws";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

// --- CẤU HÌNH ---
const PORT = 3000;
const WS_URL = "wss://websocket.azhkthg1.net/websocket?token=";
const TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJzc2NoaWNobWVtIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6MzI2OTA1OTg1LCJhZmZJZCI6InN1bndpbiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoic3VuLndpbiIsInRpbWVzdGFtcCI6MTc2NTQ2OTYxNjg3MCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDAyOjgwMDo2ZjVmOmNiYzU6ODRjMTo2YzQzOjhmZGQ6NDdkYSIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMTkucG5nIiwicGxhdGZvcm1JZCI6MiwidXNlcklkIjoiOWQyMTliNGYtMjQxYS00ZmU2LTkyNDItMDQ5MWYxYzRhMDVjIiwicmVnVGltZSI6MTc2MzcyNzkwNzk0MCwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJTQ19naWF0aGluaDIxMzMifQ.XGiELjKKAgIc-0dKYjZFOlDeH2e-LC_PvrvrzPcdY1U";

// --- GLOBAL STATE ---
let rikResults = [];
let rikCurrentSession = null;
let rikWS = null;
let rikIntervalCmd = null;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- PATTERN DATABASE ĐẦY ĐỦ ---
const PATTERN_DATABASE = {
    // Cầu cơ bản (đơn giản)
    '1-1': ['tx', 'xt'],  // thay đổi sang chữ thường
    'bệt': ['tt', 'xx'],
    '2-2': ['ttxx', 'xxtt'],
    '3-3': ['tttxxx', 'xxxttt'],
    '4-4': ['ttttxxxx', 'xxxxtttt'],
    '5-5': ['tttttxxxxx', 'xxxxxttttt'],
    
    // Cầu phức tạp nâng cao
    '1-2-1': ['txxxt', 'xtttx'],
    '2-1-2': ['ttxtt', 'xxtxx'],
    '1-2-3': ['txxttt', 'xttxxx'],
    '3-2-3': ['tttxttt', 'xxxtxxx'],
    '4-2-4': ['ttttxxtttt', 'xxxxttxxxx'],
    '3-1-3': ['tttxttt', 'xxxtxxx'],
    '1-3-1': ['txtttx', 'xtxxxt'],
    '2-3-2': ['ttxxtt', 'xxttxx'],
    '3-4-3': ['tttxxxxttt', 'xxxttttxxx'],
    '4-3-4': ['ttttxxxtttt', 'xxxxtttxxxx'],
    
    // Cầu hỗn hợp phức tạp
    '1-2-1-2': ['txxxtx', 'xtttxt'],
    '2-1-2-1': ['ttxttx', 'xxtxxt'],
    '1-1-2-2': ['txttxx', 'xtxxxt'],
    '2-2-1-1': ['ttxxtx', 'xxttxx'],
    '3-2-1': ['tttxtx', 'xxxtxt'],
    '1-2-2-1': ['txxxttx', 'xtttxxt'],
    
    // Cầu đặc biệt
    'zigzag': ['txt', 'xtx'],
    'double_zigzag': ['txtxt', 'xtxtx'],
    'triple_zigzag': ['txtxtxt', 'xtxtxtx'],
    'quad_alternate': ['txtxtxtx', 'xtxtxtxt'],
    'penta_alternate': ['txtxtxtxtx', 'xtxtxtxtxt'],
    
    // Cầu chu kỳ dài
    '1-1-1-2': ['txttx', 'xtxxt'],
    '2-1-1-1': ['ttxtx', 'xxtxt'],
    '1-2-2-2': ['txxxtt', 'xtttxx'],
    '2-2-2-1': ['ttxxttx', 'xxttxx'],
    '3-3-2': ['tttxxxtt', 'xxxttxx'],
    '2-3-3': ['ttxxttt', 'xxttxxx'],
    
    // Cầu Fibonacci
    'fibonacci_1': ['t', 'x'],  // 1
    'fibonacci_2': ['tx', 'xt'],  // 1,1
    'fibonacci_3': ['txt', 'xtx'],  // 1,1,2
    'fibonacci_4': ['txttx', 'xtxxt'],  // 1,1,2,3
    'fibonacci_5': ['txttxttx', 'xtxtxxxt'],  // 1,1,2,3,5
    
    // Cầu hình học
    'triangle': ['txx', 'xtt'],
    'square': ['ttxx', 'xxtt'],
    'pentagon': ['tttxx', 'xxxtt'],
    'hexagon': ['ttttxx', 'xxxxxt'],
    
    // Cầu sóng
    'wave_2': ['ttxx', 'xxtt'],
    'wave_3': ['tttxxx', 'xxxttt'],
    'wave_4': ['ttttxxxx', 'xxxxtttt'],
    'wave_5': ['tttttxxxxx', 'xxxxxttttt'],
    
    // Cầu đảo chiều
    'reverse_1': ['ttx', 'xxt'],
    'reverse_2': ['ttxx', 'xxtt'],
    'reverse_3': ['tttxxx', 'xxxttt'],
    'reverse_4': ['ttttxxxx', 'xxxxtttt'],
    
    // Cầu giao thoa
    'interlace_1': ['txtxt', 'xtxtx'],
    'interlace_2': ['ttxxtt', 'xxttxx'],
    'interlace_3': ['tttxxttt', 'xxxxtxxx'],
    
    // Cầu phân nhánh
    'branch_1': ['ttxtx', 'xxtxt'],
    'branch_2': ['ttxxttx', 'xxttxx'],
    'branch_3': ['tttxxtttx', 'xxxxtxxxt'],
    
    // Cầu xoắn ốc
    'spiral_1': ['txxxt', 'xtttx'],
    'spiral_2': ['ttxxxtt', 'xxtttxx'],
    'spiral_3': ['tttxxxxttt', 'xxxttttxxx'],
    
    // Cầu cấp số cộng
    'arithmetic_1': ['tx', 'xt'],
    'arithmetic_2': ['txx', 'xtt'],
    'arithmetic_3': ['txxx', 'xttt'],
    'arithmetic_4': ['txxxx', 'xtttt'],
    
    // Cầu cấp số nhân
    'geometric_1': ['tx', 'xt'],
    'geometric_2': ['txx', 'xtt'],
    'geometric_3': ['txxx', 'xttt'],
    'geometric_4': ['txxxx', 'xtttt'],
    
    // Cầu hỗn hợp đa dạng
    'mixed_1': ['ttxtxx', 'xxtxtt'],
    'mixed_2': ['txxxttx', 'xtttxxt'],
    'mixed_3': ['tttxxtxx', 'xxxxttxx'],
    'mixed_4': ['txttxtxt', 'xtxtxtxt'],
    'mixed_5': ['ttxxtxtt', 'xxtxtxxt'],
    
    // Cầu đối xứng
    'symmetry_1': ['txt', 'xtx'],
    'symmetry_2': ['ttxxtt', 'xxttxx'],
    'symmetry_3': ['tttxxxttt', 'xxxxttxxx'],
    'symmetry_4': ['ttttxxxxtttt', 'xxxxxtttxxxx'],
    
    // Cầu lặp lại
    'repeat_1': ['tt', 'xx'],
    'repeat_2': ['tttt', 'xxxx'],
    'repeat_3': ['tttttt', 'xxxxxx'],
    'repeat_4': ['tttttttt', 'xxxxxxxx'],
    
    // Cầu xen kẽ
    'alternate_1': ['txtx', 'xtxt'],
    'alternate_2': ['txtxtx', 'xtxtxt'],
    'alternate_3': ['txtxtxtx', 'xtxtxtxt'],
    'alternate_4': ['txtxtxtxtx', 'xtxtxtxtxt'],
};

// --- UTILITIES ---
function parseLines(lines) {
    try {
        const arr = lines.map(l => (typeof l === 'string' ? JSON.parse(l) : l));
        return arr.map(item => ({
            session: Number(item.session) || 0,
            dice: Array.isArray(item.dice) ? item.dice : [],
            total: Number(item.total) || 0,
            result: item.result || '',
            tx: (Number(item.total) || 0) >= 11 ? 'T' : 'X'
        })).sort((a, b) => a.session - b.session);
    } catch (e) {
        console.error("Lỗi parseLines:", e.message);
        return [];
    }
}

// --- THUẬT TOÁN AI TỐI ƯU CAO CẤP ---

/**
 * 1. ULTRA PATTERN RECOGNITION với Deep Learning
 * Nhận diện 100+ mẫu cầu và thích nghi thời gian thực
 */
function algo1_ultraPatternRecognition(history) {
    const tx = history.map(h => h.tx);
    if (tx.length < 30) return null;

    // Chuyển sang chữ thường để so sánh
    const txLower = tx.map(t => t.toLowerCase());
    const recentPattern = txLower.slice(-8).join('');
    const fullPattern = txLower.join('');
    
    let patternMatches = { t: 0, x: 0 };
    let totalWeight = 0;
    
    // Quét toàn bộ pattern database
    Object.entries(PATTERN_DATABASE).forEach(([patternName, patternList]) => {
        patternList.forEach(pattern => {
            const patternLength = pattern.length;
            if (patternLength > 8) return; // Giới hạn độ dài pattern
            
            // Tìm pattern trong lịch sử
            for (let i = 0; i <= fullPattern.length - patternLength - 1; i++) {
                if (fullPattern.substr(i, patternLength) === pattern) {
                    const nextChar = fullPattern.charAt(i + patternLength);
                    if (nextChar === 't' || nextChar === 'x') {
                        // Trọng số theo độ dài và độ phức tạp
                        const weight = (patternLength / 8) * (patternName.includes('complex') ? 1.5 : 1);
                        patternMatches[nextChar] += weight;
                        totalWeight += weight;
                    }
                }
            }
        });
    });
    
    if (totalWeight === 0) return null;
    
    // Tính xác suất với ngưỡng thích nghi
    const threshold = 0.65 + (Math.min(totalWeight, 50) / 100); // Ngưỡng động
    const tProb = patternMatches.t / totalWeight;
    const xProb = patternMatches.x / totalWeight;
    
    if (tProb >= threshold) return 'T';
    if (xProb >= threshold) return 'X';
    
    return null;
}

/**
 * 2. QUANTUM ADAPTIVE AI - Học máy lượng tử
 */
function algo2_quantumAdaptiveAI(history) {
    if (history.length < 40) return null;
    
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    
    // Trạng thái lượng tử
    const quantumState = {
        t: 0.5,
        x: 0.5
    };
    
    // Học từ lịch sử gần đây (20 phiên)
    const recentCount = Math.min(20, history.length);
    for (let i = history.length - recentCount; i < history.length; i++) {
        const weight = 0.04; // Learning rate
        if (tx[i] === 'T') {
            quantumState.t = quantumState.t * (1 + weight);
            quantumState.x = quantumState.x * (1 - weight);
        } else {
            quantumState.x = quantumState.x * (1 + weight);
            quantumState.t = quantumState.t * (1 - weight);
        }
    }
    
    // Điều chỉnh theo điểm số
    const recentAvg = totals.slice(-10).reduce((a, b) => a + b, 0) / 10;
    if (recentAvg > 11.2) {
        quantumState.t *= 0.85;
        quantumState.x *= 1.15;
    } else if (recentAvg < 9.8) {
        quantumState.t *= 1.15;
        quantumState.x *= 0.85;
    }
    
    // Chuẩn hóa
    const total = quantumState.t + quantumState.x;
    quantumState.t /= total;
    quantumState.x /= total;
    
    // Ngưỡng quyết định
    const decisionThreshold = 0.68;
    if (quantumState.t > decisionThreshold) return 'T';
    if (quantumState.x > decisionThreshold) return 'X';
    
    return null;
}

/**
 * 3. DEEP TREND ANALYSIS - Phân tích xu hướng sâu
 */
function algo3_deepTrendAnalysis(history) {
    if (history.length < 25) return null;
    
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    
    // Phân tích nhiều khung thời gian
    const periods = [5, 10, 15, 20];
    const trends = { t: 0, x: 0 };
    
    periods.forEach(period => {
        if (tx.length >= period) {
            const recent = tx.slice(-period);
            const tCount = recent.filter(c => c === 'T').length;
            const xCount = recent.filter(c => c === 'X').length;
            
            if (tCount > xCount) trends.t += 1;
            else if (xCount > tCount) trends.x += 1;
        }
    });
    
    // Phân tích điểm số
    const totalAvg = totals.reduce((a, b) => a + b, 0) / totals.length;
    const recentAvg = totals.slice(-8).reduce((a, b) => a + b, 0) / 8;
    
    if (recentAvg > totalAvg + 0.8) trends.t += 1.5;
    if (recentAvg < totalAvg - 0.8) trends.x += 1.5;
    
    // Quyết định
    if (trends.t > trends.x + 1.5) return 'T';
    if (trends.x > trends.t + 1.5) return 'X';
    
    return null;
}

/**
 * 4. SMART BRIDGE DETECTION - Phát hiện cầu thông minh
 */
function algo4_smartBridgeDetection(history) {
    const tx = history.map(h => h.tx);
    if (tx.length < 15) return null;
    
    const recentTx = tx.slice(-15);
    const lastResult = recentTx[recentTx.length - 1];
    
    // Phân tích độ dài cầu hiện tại
    let runLength = 1;
    for (let i = recentTx.length - 2; i >= 0; i--) {
        if (recentTx[i] === lastResult) runLength++;
        else break;
    }
    
    // AI THEO CẦU THÔNG MINH
    if (runLength >= 2 && runLength <= 4) {
        // Kiểm tra xem có đang trong pattern mạnh không
        const patternStr = recentTx.slice(-8).join('').toLowerCase();
        const strongPatterns = ['tttt', 'xxxx', 'txtxtx', 'xtxtxt'];
        
        let inStrongPattern = false;
        strongPatterns.forEach(pattern => {
            if (patternStr.includes(pattern)) inStrongPattern = true;
        });
        
        if (inStrongPattern) {
            return lastResult; // Tiếp tục theo cầu mạnh
        }
        
        // Kiểm tra xu hướng tổng thể
        const overallTrend = calculateOverallTrend(tx);
        if (overallTrend === lastResult) {
            return lastResult; // Theo xu hướng tổng thể
        }
    }
    
    // AI BẺ CẦU THÔNG MINH
    if (runLength >= 5) {
        // Bẻ cầu khi quá dài
        return lastResult === 'T' ? 'X' : 'T';
    }
    
    // Phát hiện pattern đảo chiều
    const lastPattern = recentTx.slice(-6).join('').toLowerCase();
    const reversalPatterns = ['tttxxx', 'xxxttt', 'ttxx', 'xxtt', 'txtxtx', 'xtxtxt'];
    
    if (reversalPatterns.includes(lastPattern)) {
        return lastResult === 'T' ? 'X' : 'T';
    }
    
    return null;
}

/**
 * 5. VOLATILITY PREDICTION - Dự đoán biến động
 */
function algo5_volatilityPrediction(history) {
    if (history.length < 30) return null;
    
    const totals = history.map(h => h.total);
    const recent10 = totals.slice(-10);
    const recent20 = totals.slice(-20);
    
    // Tính biến động
    const vol10 = calculateVolatility(recent10);
    const vol20 = calculateVolatility(recent20);
    
    // Phân tích xu hướng biến động
    if (vol10 > vol20 * 1.5) {
        // Biến động tăng -> dự đoán hồi quy
        const avgRecent = recent10.reduce((a, b) => a + b, 0) / 10;
        if (avgRecent > 11.0) return 'X';
        if (avgRecent < 10.0) return 'T';
    } else if (vol10 < vol20 * 0.7) {
        // Biến động giảm -> tiếp tục xu hướng
        const recentTx = history.slice(-10).map(h => h.tx);
        const tCount = recentTx.filter(t => t === 'T').length;
        const xCount = recentTx.filter(t => t === 'X').length;
        
        if (tCount > xCount + 2) return 'T';
        if (xCount > tCount + 2) return 'X';
    }
    
    return null;
}

/**
 * 6. PATTERN FUSION AI - Kết hợp đa mẫu
 */
function algo6_patternFusionAI(history) {
    const tx = history.map(h => h.tx);
    if (tx.length < 35) return null;
    
    const txLower = tx.map(t => t.toLowerCase());
    const patterns = [];
    
    // Phát hiện nhiều loại pattern cùng lúc
    const patternTypes = [
        { name: 'basic', length: 3, weight: 0.3 },
        { name: 'advanced', length: 5, weight: 0.5 },
        { name: 'complex', length: 7, weight: 0.7 }
    ];
    
    patternTypes.forEach(type => {
        if (txLower.length >= type.length + 1) {
            const lastPattern = txLower.slice(-type.length).join('');
            let matches = { t: 0, x: 0 };
            
            for (let i = 0; i <= txLower.length - type.length - 1; i++) {
                if (txLower.slice(i, i + type.length).join('') === lastPattern) {
                    const nextChar = txLower[i + type.length];
                    matches[nextChar]++;
                }
            }
            
            const total = matches.t + matches.x;
            if (total >= 2) {
                const confidence = Math.max(matches.t, matches.x) / total;
                if (confidence > 0.7) {
                    patterns.push({
                        prediction: matches.t > matches.x ? 'T' : 'X',
                        confidence: confidence * type.weight,
                        weight: type.weight
                    });
                }
            }
        }
    });
    
    if (patterns.length === 0) return null;
    
    // Kết hợp các dự đoán
    const combined = { t: 0, x: 0 };
    patterns.forEach(p => {
        if (p.prediction === 'T') combined.t += p.confidence;
        else combined.x += p.confidence;
    });
    
    if (combined.t > combined.x * 1.3) return 'T';
    if (combined.x > combined.t * 1.3) return 'X';
    
    return null;
}

/**
 * 7. REAL-TIME ADAPTIVE AI - AI thích nghi thời gian thực
 */
function algo7_realtimeAdaptiveAI(history) {
    if (history.length < 20) return null;
    
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    
    // Tính toán đa chỉ số
    const indicators = {
        rsi: calculateRSI(tx.slice(-14)),
        macd: calculateMACD(totals),
        bias: calculateBias(tx.slice(-20)),
        momentum: calculateMomentum(totals.slice(-10))
    };
    
    let tScore = 0;
    let xScore = 0;
    
    // Đánh giá RSI
    if (indicators.rsi > 70) xScore += 1.5;
    else if (indicators.rsi < 30) tScore += 1.5;
    
    // Đánh giá MACD
    if (indicators.macd > 0.5) tScore += 1;
    else if (indicators.macd < -0.5) xScore += 1;
    
    // Đánh giá Bias
    if (indicators.bias > 0.6) tScore += 1.2;
    else if (indicators.bias < 0.4) xScore += 1.2;
    
    // Đánh giá Momentum
    if (indicators.momentum > 0.3) tScore += 0.8;
    else if (indicators.momentum < -0.3) xScore += 0.8;
    
    // Quyết định
    if (tScore > xScore + 1.5) return 'T';
    if (xScore > tScore + 1.5) return 'X';
    
    return null;
}

// --- HELPER FUNCTIONS ---
function calculateVolatility(numbers) {
    const mean = numbers.reduce((a, b) => a + b, 0) / numbers.length;
    const variance = numbers.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / numbers.length;
    return Math.sqrt(variance);
}

function calculateOverallTrend(txArray) {
    if (txArray.length < 10) return null;
    
    const tCount = txArray.filter(t => t === 'T').length;
    const xCount = txArray.filter(t => t === 'X').length;
    
    if (tCount > xCount * 1.3) return 'T';
    if (xCount > tCount * 1.3) return 'X';
    
    return null;
}

function calculateRSI(txArray) {
    if (txArray.length < 14) return 50;
    
    let gains = 0;
    let losses = 0;
    
    for (let i = 1; i < txArray.length; i++) {
        if (txArray[i] === 'T' && txArray[i-1] === 'X') gains++;
        else if (txArray[i] === 'X' && txArray[i-1] === 'T') losses++;
    }
    
    if (losses === 0) return 100;
    const rs = gains / losses;
    return 100 - (100 / (1 + rs));
}

function calculateMACD(totals) {
    if (totals.length < 26) return 0;
    
    const ema12 = calculateEMA(totals.slice(-12), 12);
    const ema26 = calculateEMA(totals.slice(-26), 26);
    
    return ema12 - ema26;
}

function calculateEMA(numbers, period) {
    const multiplier = 2 / (period + 1);
    let ema = numbers[0];
    
    for (let i = 1; i < numbers.length; i++) {
        ema = numbers[i] * multiplier + ema * (1 - multiplier);
    }
    
    return ema;
}

function calculateBias(txArray) {
    const tCount = txArray.filter(t => t === 'T').length;
    return tCount / txArray.length;
}

function calculateMomentum(numbers) {
    if (numbers.length < 2) return 0;
    return numbers[numbers.length - 1] - numbers[0];
}

// --- DANH SÁCH THUẬT TOÁN TỐI ƯU ---
const ALGORITHMS = [
    { id: 'ultra_pattern', fn: algo1_ultraPatternRecognition, name: 'Ultra Pattern AI' },
    { id: 'quantum_ai', fn: algo2_quantumAdaptiveAI, name: 'Quantum Adaptive AI' },
    { id: 'deep_trend', fn: algo3_deepTrendAnalysis, name: 'Deep Trend AI' },
    { id: 'smart_bridge', fn: algo4_smartBridgeDetection, name: 'Smart Bridge AI' },
    { id: 'volatility', fn: algo5_volatilityPrediction, name: 'Volatility AI' },
    { id: 'pattern_fusion', fn: algo6_patternFusionAI, name: 'Pattern Fusion AI' },
    { id: 'realtime_ai', fn: algo7_realtimeAdaptiveAI, name: 'Real-time Adaptive AI' },
];

// --- ADVANCED AI CORE ---
class AdvancedDeepLearningAI {
    constructor() {
        this.history = [];
        this.algorithmWeights = {};
        this.algorithmPerformance = {};
        this.recentPredictions = {};
        this.learningRate = 0.1;
        
        ALGORITHMS.forEach(algo => {
            this.algorithmWeights[algo.id] = 1.0;
            this.algorithmPerformance[algo.id] = {
                correct: 0,
                total: 0,
                recent: [],
                streak: 0,
                maxStreak: 0,
                name: algo.name
            };
            this.recentPredictions[algo.id] = null;
        });
    }
    
    updateAlgorithmPerformance(actualTx) {
        ALGORITHMS.forEach(algo => {
            const perf = this.algorithmPerformance[algo.id];
            const lastPred = this.recentPredictions[algo.id];
            
            if (lastPred) {
                const correct = lastPred === actualTx;
                
                perf.correct += correct ? 1 : 0;
                perf.total += 1;
                
                if (correct) {
                    perf.streak++;
                    perf.maxStreak = Math.max(perf.maxStreak, perf.streak);
                } else {
                    perf.streak = 0;
                }
                
                perf.recent.push(correct ? 1 : 0);
                if (perf.recent.length > 10) {
                    perf.recent.shift();
                }
                
                if (perf.total >= 15) {
                    const accuracy = perf.correct / perf.total;
                    const recentAccuracy = perf.recent.reduce((a, b) => a + b) / perf.recent.length;
                    const streakBonus = perf.streak * 0.03;
                    
                    let newWeight = (accuracy * 0.6 + recentAccuracy * 0.3 + streakBonus * 0.1);
                    newWeight = Math.max(0.1, Math.min(2.0, newWeight * 1.8));
                    
                    this.algorithmWeights[algo.id] = 
                        this.algorithmWeights[algo.id] * 0.8 + newWeight * 0.2;
                }
            }
        });
        
        ALGORITHMS.forEach(algo => { this.recentPredictions[algo.id] = null; });
    }
    
    calculateTrueConfidence(predictions) {
        if (predictions.length === 0) return 0.5;
        
        const votes = { T: 0, X: 0 };
        let totalWeight = 0;
        
        predictions.forEach(pred => {
            const weight = this.algorithmWeights[pred.algorithm] || 1.0;
            votes[pred.prediction] += weight;
            totalWeight += weight;
        });
        
        if (totalWeight === 0) return 0.5;
        
        const tVotes = votes['T'] || 0;
        const xVotes = votes['X'] || 0;
        const winningPrediction = tVotes > xVotes ? 'T' : (xVotes > tVotes ? 'X' : null);
        
        if (!winningPrediction) return 0.5;
        
        const winningVotes = Math.max(tVotes, xVotes);
        let confidence = winningVotes / totalWeight;
        
        // Điều chỉnh confidence dựa trên số thuật toán đồng thuận
        const consensus = predictions.filter(p => 
            p.prediction === winningPrediction).length / predictions.length;
        
        confidence = (confidence * 0.7) + (consensus * 0.3);
        
        return Math.max(0.5, Math.min(0.98, confidence));
    }
    
    predict() {
        if (this.history.length < 15) {
            return {
                prediction: 'tài',
                confidence: 0.5,
                rawPrediction: 'T',
                algorithms: 0,
            };
        }
        
        const predictions = [];
        this.recentPredictions = {};
        
        ALGORITHMS.forEach(algo => {
            try {
                const pred = algo.fn(this.history);
                if (pred === 'T' || pred === 'X') {
                    const weight = this.algorithmWeights[algo.id] || 1.0;
                    predictions.push({
                        algorithm: algo.id,
                        prediction: pred,
                        weight: weight
                    });
                    this.recentPredictions[algo.id] = pred;
                }
            } catch (e) {
                console.error(`Lỗi thuật toán ${algo.id}:`, e.message);
            }
        });
        
        if (predictions.length === 0) {
            return {
                prediction: 'tài',
                confidence: 0.5,
                rawPrediction: 'T',
                algorithms: 0,
            };
        }
        
        const votes = { T: 0, X: 0 };
        predictions.forEach(p => {
            votes[p.prediction] += p.weight;
        });
        
        const tVotes = votes['T'] || 0;
        const xVotes = votes['X'] || 0;
        
        let finalPrediction = 'T';
        if (xVotes > tVotes) {
            finalPrediction = 'X';
        } else if (xVotes === tVotes) {
            finalPrediction = this.history[this.history.length - 1].tx;
        }
        
        const confidence = this.calculateTrueConfidence(predictions);
        
        return {
            prediction: finalPrediction === 'T' ? 'tài' : 'xỉu',
            confidence: confidence,
            rawPrediction: finalPrediction,
            algorithms: predictions.length,
        };
    }
    
    addResult(record) {
        const parsed = {
            session: Number(record.session) || 0,
            dice: Array.isArray(record.dice) ? record.dice : [],
            total: Number(record.total) || 0,
            result: record.result || '',
            tx: (Number(record.total) || 0) >= 11 ? 'T' : 'X'
        };
        
        if (this.history.length >= 15) {
            this.updateAlgorithmPerformance(parsed.tx);
        }
        
        this.history.push(parsed);
        if (this.history.length > 500) {
            this.history = this.history.slice(-400);
        }
        
        return parsed;
    }
    
    loadHistory(historyData) {
        this.history = parseLines(historyData);
        
        if (this.history.length >= 30) {
            console.log(`🤖 Đang huấn luyện AI trên ${this.history.length} mẫu...`);
            
            for (let i = 20; i < this.history.length - 1; i++) {
                const pastHistory = this.history.slice(0, i + 1);
                const actualTx = this.history[i + 1]?.tx;
                
                if (!actualTx) continue;
                
                ALGORITHMS.forEach(algo => {
                    try {
                        const pred = algo.fn(pastHistory);
                        if (pred) {
                            const perf = this.algorithmPerformance[algo.id];
                            const correct = pred === actualTx;
                            
                            perf.recent.push(correct ? 1 : 0);
                            if (perf.recent.length > 10) {
                                perf.recent.shift();
                            }
                            perf.correct += correct ? 1 : 0;
                            perf.total++;
                            
                            if (perf.total >= 15) {
                                const accuracy = perf.correct / perf.total;
                                const recentAccuracy = perf.recent.reduce((a, b) => a + b) / perf.recent.length;
                                let newWeight = (accuracy * 0.6 + recentAccuracy * 0.3);
                                newWeight = Math.max(0.1, Math.min(2.0, newWeight * 1.8));
                                this.algorithmWeights[algo.id] = newWeight;
                            }
                        }
                    } catch (e) {
                        // Bỏ qua lỗi
                    }
                });
            }
            
            console.log('✅ Huấn luyện AI hoàn tất!');
        }
    }
    
    getPattern() {
        if (this.history.length < 50) return { recent: 'đang thu thập...', long: 'đang thu thập...' };
        const tx = this.history.map(h => h.tx);
        // Chuyển sang chữ thường
        const recent = tx.slice(-20).join('').toLowerCase();
        const long = tx.slice(-50).join('').toLowerCase();
        
        return {
            recent: recent,
            long: long,
            discovered: this.discoverDominantPattern(tx.slice(-30))
        };
    }
    
    discoverDominantPattern(txArray) {
        const str = txArray.join('').toLowerCase();
        let dominantPattern = null;
        let maxOccurrences = 0;
        
        Object.entries(PATTERN_DATABASE).forEach(([name, patterns]) => {
            patterns.forEach(pattern => {
                let count = 0;
                for (let i = 0; i <= str.length - pattern.length; i++) {
                    if (str.substr(i, pattern.length) === pattern) {
                        count++;
                    }
                }
                
                if (count > maxOccurrences) {
                    maxOccurrences = count;
                    dominantPattern = name;
                }
            });
        });
        
        return dominantPattern || 'không xác định';
    }
    
    getStats() {
        const stats = {};
        ALGORITHMS.forEach(algo => {
            const perf = this.algorithmPerformance[algo.id];
            if (perf.total > 0) {
                stats[algo.id] = {
                    name: perf.name,
                    accuracy: (perf.correct / perf.total * 100).toFixed(1) + '%',
                    weight: this.algorithmWeights[algo.id].toFixed(2),
                    predictions: perf.total,
                    streak: perf.streak
                };
            }
        });
        
        return stats;
    }
}

// --- Khởi tạo AI ---
const ai = new AdvancedDeepLearningAI();

// --- API SERVER ---
const app = fastify({ 
    logger: false 
});

await app.register(cors, { 
    origin: "*" 
});

// GET /api/taixiu/sunwin
app.get("/api/taixiu/sunwin", async (request, reply) => {
    try {
        const valid = rikResults.filter((r) => r.dice?.length === 3);
        const lastResult = valid.length ? valid[0] : null;
        const currentPrediction = ai.predict();
        const pattern = ai.getPattern();

        if (!lastResult) {
            return {
                id: "GiaThinhzZz Sunwin AI",
                status: "đang chờ dữ liệu phiên đầu tiên...",
                phien_truoc: null,
                tong: null,
                ket_qua: "đang chờ...",
                pattern_gan_nhat: pattern.recent,
                pattern_dai: pattern.long,
                phien_hien_tai: null,
                du_doan: "đang tính...",
                do_tin_cay_ai: "50%",
            };
        }

        return {
            id: "GiaThinhzZz Sunwin AI",
            phien_truoc: lastResult.session,
            xuc_xac: lastResult.dice,
            tong: lastResult.total,
            ket_qua: lastResult.result.toLowerCase(),
            pattern_gan_nhat: pattern.recent,
            pattern_dai: pattern.long,
            pattern_chu_dao: pattern.discovered,
            phien_hien_tai: lastResult.session + 1,
            du_doan: currentPrediction.prediction,
            do_tin_cay_ai: `${(currentPrediction.confidence * 100).toFixed(1)}%`,
        };
    } catch (error) {
        console.error('Lỗi API /api/taixiu/sunwin:', error);
        return {
            id: "GiaThinhzZz Sunwin AI",
            error: "Hệ thống đang xử lý lỗi hoặc chưa đủ dữ liệu."
        };
    }
});

// GET /api/taixiu/history
app.get("/api/taixiu/history", async () => { 
    try {
        const valid = rikResults.filter((r) => r.dice?.length === 3);
        if (!valid.length) return { message: "chưa có dữ liệu." };
        
        return valid.slice(0, 30).map((i) => ({
            session: i.session,
            dice: i.dice,
            total: i.total,
            result: i.result.toLowerCase(),
            tx: i.total >= 11 ? 'T' : 'X'
        }));
    } catch (e) {
        console.error('Lỗi API /api/taixiu/history:', e);
        return { message: "lỗi hệ thống" };
    }
});

// GET /api/taixiu/ai-stats
app.get("/api/taixiu/ai-stats", async () => {
    try {
        const stats = ai.getStats();
        const prediction = ai.predict();
        const pattern = ai.getPattern();
        
        return {
            status: "online",
            ai_version: "9.0 - Ultra Pattern Recognition",
            current_prediction: prediction.prediction,
            confidence: `${(prediction.confidence * 100).toFixed(1)}%`,
            algorithms_active: prediction.algorithms,
            pattern_dominant: pattern.discovered,
            algorithm_stats: stats
        };
    } catch (e) {
        console.error('Lỗi API /api/taixiu/ai-stats:', e);
        return { error: "Lỗi hệ thống" };
    }
});

// GET /
app.get("/", async () => { 
    return {
        status: "online",
        name: "GiaThinhzZz Sunwin AI",
        version: "9.0 - Ultra Pattern Recognition & Quantum AI",
        description: "Hệ thống AI dự đoán với 100+ mẫu cầu và học máy nâng cao",
        algorithms_count: ALGORITHMS.length,
        pattern_database: Object.keys(PATTERN_DATABASE).length + " mẫu cầu",
        features: [
            "Ultra Pattern Recognition (100+ mẫu)",
            "Quantum Adaptive AI",
            "Smart Bridge Detection",
            "Real-time Adaptive Learning",
            "Multi-layer Pattern Fusion"
        ]
    };
});

// --- SERVER START ---
const start = async () => {
    try {
        await app.listen({
            port: PORT,
            host: "0.0.0.0"
        });
        console.log(`\n==============================================`);
        console.log(`🚀 GiaThinhzZz Sunwin AI ULTRA Server`);
        console.log(`==============================================`);
        console.log(`   Port: ${PORT}`);
        console.log(`   Thuật toán: ${ALGORITHMS.length} AI Algorithms`);
        console.log(`   Pattern Database: ${Object.keys(PATTERN_DATABASE).length} mẫu`);
        console.log(`   Features: Quantum AI + Smart Bridge + Deep Learning`);
        console.log(`==============================================`);
    } catch (err) {
        console.error('❌ Lỗi khởi động server:', err);
        process.exit(1);
    }
};

// --- WEBSOCKET HANDLERS ---
function decodeBinaryMessage(data) {
    try {
        const message = new TextDecoder().decode(data);
        if (message.startsWith("[") || message.startsWith("{")) {
            return JSON.parse(message);
        }
        return null;
    } catch {
        return null;
    }
}

function sendRikCmd1005() {
    if (rikWS?.readyState === WebSocket.OPEN) {
        try {
            rikWS.send(JSON.stringify([6, "MiniGame", "taixiuPlugin", {
                cmd: 1005
            }]));
        } catch (e) {
            console.error("Lỗi gửi lệnh 1005:", e.message);
        }
    }
}

function connectRikWebSocket() {
    console.log("\n🔌 Đang kết nối WebSocket...");
    
    if (rikWS && (rikWS.readyState === WebSocket.OPEN || rikWS.readyState === WebSocket.CONNECTING)) {
        rikWS.close();
    }
    clearInterval(rikIntervalCmd);

    try {
        rikWS = new WebSocket(`${WS_URL}${TOKEN}`);
    } catch (e) {
         console.error("Lỗi tạo WebSocket:", e.message);
         setTimeout(connectRikWebSocket, 5000);
         return;
    }

    rikWS.on("open", () => {
        console.log("✅ WebSocket connected - Đang xác thực...");
        
        const authPayload = [1, "MiniGame", "SC_giathinh2133", "thinh211", {
            info: JSON.stringify({
                ipAddress: "2402:800:62cd:b4d1:8c64:a3c9:12bf:c19a",
                wsToken: TOKEN,
                userId: "cdbaf598-e4ef-47f8-b4a6-a4881098db86",
                username: "SC_hellokietne212",
                timestamp: Date.now(),
            }),
            signature: "473ABDDDA6BDD74D8F0B6036223B0E3A002A518203A9BB9F95AD763E3BF969EC2CBBA61ED1A3A9E217B52A4055658D7BEA38F89B806285974C7F3F62A9400066709B4746585887D00C9796552671894F826E69EFD234F6778A5DDC24830CEF68D51217EF047644E0B0EB1CB26942EB34AEF114AEC36A6DF833BB10F7D122EA5E",
            pid: 5,
            subi: true,
        }];
        
        try {
             rikWS.send(JSON.stringify(authPayload));
        } catch (e) {
             console.error("Lỗi gửi xác thực:", e.message);
        }
       
        rikIntervalCmd = setInterval(sendRikCmd1005, 5000);
    });

    rikWS.on("message", (data) => {
        try {
            const json = typeof data === "string" ? JSON.parse(data) : decodeBinaryMessage(data);
            if (!json) return;

            if (json.session && Array.isArray(json.dice)) {
                const record = {
                    session: json.session,
                    dice: json.dice,
                    total: json.total,
                    result: json.result,
                };
                
                const parsed = ai.addResult(record);
                
                if (!rikCurrentSession || record.session > rikCurrentSession) {
                    rikCurrentSession = record.session;
                    rikResults.unshift(record);
                    if (rikResults.length > 100) rikResults.pop();
                }
                
                const prediction = ai.predict();
                console.log(`\n==============================================`);
                console.log(`📥 PHIÊN ${parsed.session}: ${parsed.result} (${parsed.total})`);
                console.log(`🔮 DỰ ĐOÁN ${parsed.session + 1}: **${prediction.prediction.toUpperCase()}**`);
                console.log(`🎯 CONFIDENCE: ${(prediction.confidence * 100).toFixed(1)}%`);
                console.log(`🤖 ALGORITHMS: ${prediction.algorithms}/${ALGORITHMS.length}`);
                
            } 
            else if (Array.isArray(json) && json[1]?.htr) {
                const newHistory = json[1].htr
                    .map((i) => ({
                        session: i.sid,
                        dice: [i.d1, i.d2, i.d3],
                        total: i.d1 + i.d2 + i.d3,
                        result: i.d1 + i.d2 + i.d3 >= 11 ? "Tài" : "Xỉu",
                    }))
                    .sort((a, b) => a.session - b.session);

                ai.loadHistory(newHistory);
                rikResults = newHistory.slice(-50).sort((a, b) => b.session - a.session);

                const prediction = ai.predict();
                const stats = ai.getStats();

                console.log(`\n==============================================`);
                console.log(`📊 Đã tải ${newHistory.length} kết quả lịch sử`);
                console.log(`🤖 ULTRA PATTERN AI ĐÃ SẴN SÀNG`);
                console.log(`==============================================`);
                console.log(`🎯 Confidence: ${(prediction.confidence * 100).toFixed(1)}%`);
                
                const algoArray = Object.entries(stats)
                    .map(([key, value]) => ({ key, ...value }))
                    .sort((a, b) => parseFloat(b.weight) - parseFloat(a.weight))
                    .slice(0, 3);
                
                console.log(`📈 Top 3 thuật toán:`);
                algoArray.forEach((algo, idx) => {
                    console.log(`   ${idx + 1}. ${algo.name}: WGT ${algo.weight} | ACC ${algo.accuracy}`);
                });
            }
        } catch (e) {
            console.error("❌ Parse message error:", e.message);
        }
    });

    rikWS.on("close", () => {
        console.log("🔌 WebSocket disconnected. Reconnecting in 3s...");
        clearInterval(rikIntervalCmd);
        setTimeout(connectRikWebSocket, 3000);
    });

    rikWS.on("error", (err) => {
        console.error("🔌 WebSocket error:", err.message);
        rikWS.close();
    });
}

// Khởi động Server và WebSocket
start().then(() => {
    connectRikWebSocket();
}).catch(err => {
    console.error('Failed to start application:', err);
});
