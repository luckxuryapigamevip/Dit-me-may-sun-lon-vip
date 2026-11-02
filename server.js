import fastify from "fastify";
import cors from "@fastify/cors";
import WebSocket from "ws";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

// ============================================================
// CẤU HÌNH
// ============================================================
const PORT    = process.env.PORT || 3000;
const WS_URL  = "wss://websocket.azhkthg1.net/websocket?token=";
const TOKEN   = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJzc2NoaWNobWVtIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6MzI2OTA1OTg1LCJhZmZJZCI6InN1bndpbiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoic3VuLndpbiIsInRpbWVzdGFtcCI6MTc2NTQ2OTYxNjg3MCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDAyOjgwMDo2ZjVmOmNiYzU6ODRjMTo2YzQzOjhmZGQ6NDdkYSIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMTkucG5nIiwicGxhdGZvcm1JZCI6MiwidXNlcklkIjoiOWQyMTliNGYtMjQxYS00ZmU2LTkyNDItMDQ5MWYxYzRhMDVjIiwicmVnVGltZSI6MTc2MzcyNzkwNzk0MCwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJTQ19naWF0aGluaDIxMzMifQ.XGiELjKKAgIc-0dKYjZFOlDeH2e-LC_PvrvrzPcdY1U";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ============================================================
// GLOBAL STATE
// ============================================================
let rikResults        = [];
let rikCurrentSession = null;
let rikWS             = null;
let rikIntervalCmd    = null;

// pattern_10: mảng 10 kết quả gần nhất dạng 'T' hoặc 'X'
let pattern10     = [];

// dudoanHistory: lưu 1000 phiên dự đoán đúng/sai
let dudoanHistory = [];

// pendingPred: dự đoán đang chờ kết quả xác nhận từ phiên tiếp theo
let pendingPred   = null;   // { rawPred: 'T'|'X', conf: number, phien: number }

// ============================================================
// UTILITIES
// ============================================================
const _sum  = a => a.reduce((s,v) => s+v, 0);
const _avg  = a => a.length ? _sum(a)/a.length : 0;
const _std  = a => { const m=_avg(a); return Math.sqrt(_avg(a.map(v=>Math.pow(v-m,2)))); };
const _ent  = a => {
    if (!a.length) return 0;
    const f = a.reduce((o,v)=>{o[v]=(o[v]||0)+1;return o;},{});
    return -Object.values(f).reduce((e,c)=>{ const p=c/a.length; return e+(p>0?p*Math.log2(p):0); },0);
};
const _sim  = (a,b) => {
    if (a.length!==b.length) return 0;
    return a.split('').filter((c,i)=>c===b[i]).length/a.length;
};
const _majority = obj => Object.entries(obj).reduce(
    (best,[k,v]) => v>best.val ? {key:k,val:v} : best, {key:null,val:-Infinity}
);
const _lastN = (a,n) => a.slice(Math.max(0,a.length-n));

function parseRecord(item) {
    const total = Number(item.total||0) ||
        ((item.d1||0)+(item.d2||0)+(item.d3||0));
    return {
        session: Number(item.session||item.sid||0),
        dice:    Array.isArray(item.dice)?item.dice:[item.d1||0,item.d2||0,item.d3||0],
        total,
        result:  total>=11?'TAI':'XIU',
        tx:      total>=11?'T':'X'
    };
}

function getStreakInfo(tx) {
    if (!tx.length) return {val:null,len:0};
    let len=1, val=tx.at(-1);
    for (let i=tx.length-2;i>=0;i--) { if(tx[i]===val)len++; else break; }
    return {val,len};
}

function extractFeat(history) {
    const tx     = history.map(h=>h.tx);
    const totals = history.map(h=>h.total);
    let runs=[],cur=tx[0],len=1;
    for (let i=1;i<tx.length;i++) {
        if(tx[i]===cur) len++;
        else { runs.push({val:cur,len}); cur=tx[i]; len=1; }
    }
    if(tx.length) runs.push({val:cur,len});
    return { tx, totals, runs,
        maxRun:    runs.reduce((m,r)=>Math.max(m,r.len),0),
        meanTotal: _avg(totals),
        stdTotal:  _std(totals),
        entropy:   _ent(tx)
    };
}

// ============================================================
// BẺ CẦU MANAGER — tự động flip khi sai 1 tay, tắt khi đúng
// ============================================================
class BreakCauManager {
    constructor() {
        this.isBreaking = false;
        this.breakCount = 0;
        this.log        = [];
    }
    update(predicted, actual) {
        const ok = (predicted === actual);
        if (!ok) { this.isBreaking=true;  this.breakCount++; }
        else      { this.isBreaking=false; this.breakCount=0; }
        this.log.push({predicted,actual,ok,breaking:this.isBreaking,time:new Date().toISOString()});
        if (this.log.length>100) this.log.shift();
    }
    apply(raw) {
        if (!raw) return raw;
        return this.isBreaking ? (raw==='T'?'X':'T') : raw;
    }
    status() { return {isBreaking:this.isBreaking,breakCount:this.breakCount}; }
}

// ============================================================
// ADAPTIVE MEMORY — AI-16 tự nhớ pattern ngắn
// ============================================================
const adaptiveMem = {};   // key = "TXXT..." → {T:n, X:n}

// ============================================================
// 120 THUẬT TOÁN DỰ ĐOÁN TÀI XỈU
// ============================================================

// ─── NHÓM MARKOV (bậc 1-9 + windowed) ─────────────────────
function buildMx(tx,order){
    const m={};
    for(let i=0;i<=tx.length-order-1;i++){
        const k=tx.slice(i,i+order).join(''), n=tx[i+order];
        m[k]=m[k]||{T:0,X:0}; m[k][n]++;
    }
    return m;
}
function mxP(tx,ord,minS=3,minC=0.57){
    if(tx.length<ord+minS) return null;
    const m=buildMx(tx,ord), k=tx.slice(-ord).join(''), c=m[k];
    if(!c) return null;
    const tot=c.T+c.X;
    if(tot<minS) return null;
    if(Math.max(c.T,c.X)/tot<minC) return null;
    return c.T>c.X?'T':'X';
}
const M1 = h=>mxP(h.map(r=>r.tx),1,5,0.55);
const M2 = h=>mxP(h.map(r=>r.tx),2,4,0.57);
const M3 = h=>mxP(h.map(r=>r.tx),3,3,0.60);
const M4 = h=>mxP(h.map(r=>r.tx),4,3,0.62);
const M5 = h=>mxP(h.map(r=>r.tx),5,2,0.64);
const M6 = h=>mxP(h.map(r=>r.tx),6,2,0.66);
const M7 = h=>mxP(h.map(r=>r.tx),7,2,0.68);
const M8 = h=>mxP(h.map(r=>r.tx),8,2,0.70);
const M9 = h=>mxP(h.map(r=>r.tx),9,2,0.72);
const M2W= h=>mxP(h.map(r=>r.tx).slice(-40),2,3,0.58);
const M3W= h=>mxP(h.map(r=>r.tx).slice(-50),3,3,0.60);
const M4W= h=>mxP(h.map(r=>r.tx).slice(-60),4,2,0.62);

// ─── NHÓM N-GRAM ────────────────────────────────────────────
function ngP(tx,k,minM=3,minD=2){
    if(tx.length<k+minM) return null;
    const last=tx.slice(-k).join(''); let T=0,X=0;
    for(let i=0;i<=tx.length-k-1;i++){
        if(tx.slice(i,i+k).join('')===last){tx[i+k]==='T'?T++:X++;}
    }
    if(T+X<minM||Math.abs(T-X)<minD) return null;
    return T>X?'T':'X';
}
const NG3 = h=>ngP(h.map(r=>r.tx),3,3,2);
const NG4 = h=>ngP(h.map(r=>r.tx),4,3,2);
const NG5 = h=>ngP(h.map(r=>r.tx),5,2,1);
const NG6 = h=>ngP(h.map(r=>r.tx),6,2,1);
const NG7 = h=>ngP(h.map(r=>r.tx),7,2,1);
const NG3W= h=>ngP(h.map(r=>r.tx).slice(-40),3,2,1);
const NG4W= h=>ngP(h.map(r=>r.tx).slice(-50),4,2,1);
const NGFz= h=>{
    const tx=h.map(r=>r.tx); if(tx.length<20) return null;
    const last=tx.slice(-5).join(''); let T=0,X=0;
    for(let i=0;i<=tx.length-6;i++){
        const s=_sim(tx.slice(i,i+5).join(''),last);
        if(s>=0.80){tx[i+5]==='T'?T+=s:X+=s;}
    }
    return T===X?null:T>X?'T':'X';
};
const NGF8= h=>{
    const tx=h.map(r=>r.tx); if(tx.length<25) return null;
    const last=tx.slice(-6).join(''); let T=0,X=0;
    for(let i=0;i<=tx.length-7;i++){
        const s=_sim(tx.slice(i,i+6).join(''),last);
        if(s>=0.75){tx[i+6]==='T'?T+=s:X+=s;}
    }
    return T===X?null:T>X?'T':'X';
};

// ─── NHÓM STREAK / CẦU ──────────────────────────────────────
const SK2  = h=>{const{val,len}=getStreakInfo(h.map(r=>r.tx));return len===2?val:null;};
const SK34 = h=>{const{val,len}=getStreakInfo(h.map(r=>r.tx));return len>=3&&len<=4?val:null;};
const SK56 = h=>{const{val,len}=getStreakInfo(h.map(r=>r.tx));return len>=5&&len<=6?val:null;};
const SK7  = h=>{const{val,len}=getStreakInfo(h.map(r=>r.tx));return len>=7&&len<=9?(val==='T'?'X':'T'):null;};
const SK10 = h=>{const{val,len}=getStreakInfo(h.map(r=>r.tx));return len>=10?(val==='T'?'X':'T'):null;};
const ALT4 = h=>{
    const tx=h.map(r=>r.tx); let c=1;
    for(let i=tx.length-2;i>=0&&tx[i]!==tx[i+1];i--) c++;
    return c>=4&&c<8?(tx.at(-1)==='T'?'X':'T'):null;
};
const ALT8 = h=>{
    const tx=h.map(r=>r.tx); let c=1;
    for(let i=tx.length-2;i>=0&&tx[i]!==tx[i+1];i--) c++;
    return c>=8?tx.at(-1):null;
};
const D22  = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<8) return null;
    const r=tx.slice(-8).reverse();
    const ps=[0,2,4,6].map(i=>r[i]+r[i+1]);
    if(ps.every(p=>p==='TT'||p==='XX')&&ps[0]!==ps[1]&&ps[1]!==ps[2]&&ps[2]!==ps[3])
        return ps[3][0];
    return null;
};
const D33  = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<6) return null;
    const[a,b,c,d,e,f]=tx.slice(-6);
    return(a===b&&b===c&&d===e&&e===f&&c!==d)?c:null;
};
const D44  = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<8) return null;
    const[a,b,c,d,e,f,g,k]=tx.slice(-8);
    return(a===b&&b===c&&c===d&&e===f&&f===g&&g===k&&d!==e)?d:null;
};
const C21  = h=>{const tx=h.map(r=>r.tx);if(tx.length<3)return null;const[a,b,c]=tx.slice(-3);return a===b&&b!==c?a:null;};
const C12  = h=>{const tx=h.map(r=>r.tx);if(tx.length<3)return null;const[a,b,c]=tx.slice(-3);return a!==b&&b===c?a:null;};
const C31  = h=>{const tx=h.map(r=>r.tx);if(tx.length<4)return null;const[a,b,c,d]=tx.slice(-4);return a===b&&b===c&&c!==d?a:null;};
const C13  = h=>{const tx=h.map(r=>r.tx);if(tx.length<4)return null;const[a,b,c,d]=tx.slice(-4);return a!==b&&b===c&&c===d?b:null;};
const C41  = h=>{const tx=h.map(r=>r.tx);if(tx.length<5)return null;const l=tx.slice(-5);return l.slice(0,4).every(v=>v===l[0])&&l[4]!==l[0]?l[0]:null;};
const C14  = h=>{const tx=h.map(r=>r.tx);if(tx.length<5)return null;const l=tx.slice(-5);return l[0]!==l[1]&&l[1]===l[2]&&l[2]===l[3]&&l[3]===l[4]?l[1]:null;};

// ─── NHÓM PATTERN MAP ───────────────────────────────────────
const P4M={'TTXX':'T','XXTT':'X','TXTX':'T','XTXT':'X',
    'TTXT':'X','XXTX':'T','TXXT':'T','XTTX':'X',
    'TTTX':'T','XXXT':'X','TXTT':'X','XTXX':'T',
    'TTTT':'X','XXXX':'T','TXXX':'T','XTTT':'X'};
const P5M={'TTTXX':'T','XXXTT':'X','TTXXT':'X','XXTTX':'T',
    'TXTXT':'X','XTXTX':'T','TXTTX':'T','XTXXT':'X',
    'TTXTX':'X','XXXTX':'T','TXXTT':'T','XTTXX':'X'};
const P6M={'TTTXXX':'T','XXXTTT':'X','TTXXTT':'X','XXTXXX':'T',
    'TXTXTX':'T','XTXTXT':'X','TTXTXT':'X','XXTXTX':'T',
    'TXXTTX':'T','XTTXXT':'X','TTXTTX':'X','XXTXXT':'T'};
const P4 = h=>{const tx=h.map(r=>r.tx);if(tx.length<4)return null;return P4M[tx.slice(-4).join('')]||null;};
const P5 = h=>{const tx=h.map(r=>r.tx);if(tx.length<5)return null;return P5M[tx.slice(-5).join('')]||null;};
const P6 = h=>{const tx=h.map(r=>r.tx);if(tx.length<6)return null;return P6M[tx.slice(-6).join('')]||null;};
const P7 = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<7) return null;
    const m={'TTTXXXX':'T','XXXTTTT':'X','TXTXTXT':'X','XTXTXTX':'T',
             'TTXTTXX':'T','XXTXXTT':'X','TXXXTXT':'T','XTTTTXT':'X'};
    return m[tx.slice(-7).join('')]||null;
};
const P8 = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<8) return null;
    const m={'TTTTXXXX':'T','XXXXTTTT':'X','TXTTXTXT':'X','XTXXTXTX':'T',
             'TTXXTTXX':'T','XXTTXXTT':'X','TXTXXTXT':'T','XTXTTXTX':'X'};
    return m[tx.slice(-8).join('')]||null;
};

// ─── NHÓM THỐNG KÊ ──────────────────────────────────────────
const FR_GL= h=>{if(h.length<20)return null;const tx=h.map(r=>r.tx);const r=tx.filter(c=>c==='T').length/tx.length;return r>0.62?'X':r<0.38?'T':null;};
const FR_20= h=>{if(h.length<20)return null;const tx=h.map(r=>r.tx).slice(-20);const T=tx.filter(c=>c==='T').length;return T>=14?'X':T<=6?'T':null;};
const FR_10= h=>{if(h.length<10)return null;const tx=h.map(r=>r.tx).slice(-10);const T=tx.filter(c=>c==='T').length;return T>=8?'X':T<=2?'T':null;};
const FR_MW= h=>{
    if(h.length<20) return null;
    const tx=h.map(r=>r.tx); const v={T:0,X:0};
    for(const w of[10,20,30,50]){
        const s=tx.slice(-w); if(s.length<w) continue;
        const r=s.filter(c=>c==='T').length/s.length;
        if(r>0.62) v.X++; else if(r<0.38) v.T++;
    }
    return v.T>v.X?'T':v.X>v.T?'X':null;
};
const REG  = h=>{
    if(h.length<50) return null;
    const tx=h.map(r=>r.tx);
    const st=tx.slice(-10).filter(c=>c==='T').length/10;
    const lt=tx.filter(c=>c==='T').length/tx.length;
    const d=st-lt;
    return d>0.20?'X':d<-0.20?'T':null;
};
const MOM  = h=>{
    if(h.length<10) return null;
    const tx=h.map(r=>r.tx).slice(-10);
    let s=0; tx.forEach((v,i)=>{s+=(v==='T'?1:-1)*(i+1);});
    const n=s/55;
    return n>0.22?'T':n<-0.22?'X':null;
};
const MAC  = h=>{
    if(h.length<20) return null;
    const b=h.map(r=>r.tx).map(v=>v==='T'?1:0);
    const f=_avg(b.slice(-5)), sl=_avg(b.slice(-15));
    const d=f-sl;
    return d>0.12?'T':d<-0.12?'X':null;
};
const BAY  = h=>{
    if(h.length<15) return null;
    let p=0.5;
    for(const r of h.map(x=>x.tx)){p=r==='T'?Math.min(0.95,p+0.03):Math.max(0.05,p-0.03);}
    return p>0.62?'X':p<0.38?'T':null;
};
const ENT  = h=>{
    if(h.length<20) return null;
    const tx=h.map(r=>r.tx).slice(-20);
    const e=_ent(tx), T=tx.filter(c=>c==='T').length;
    return e<0.75?(T>10?'T':'X'):null;
};
const CHI2 = h=>{
    if(h.length<30) return null;
    const tx=h.map(r=>r.tx);
    const T=tx.filter(c=>c==='T').length, exp=tx.length/2;
    const c=Math.pow(T-exp,2)/exp+Math.pow((tx.length-T)-exp,2)/exp;
    return c>3.84?(T>tx.length/2?'X':'T'):null;
};
const HC   = h=>{
    if(h.length<30) return null;
    const tx=h.map(r=>r.tx);
    const r20=tx.slice(-20).filter(c=>c==='T').length/20;
    const r50=tx.slice(-Math.min(50,tx.length)).filter(c=>c==='T').length/Math.min(50,tx.length);
    const d=r20-r50;
    return d>0.18?'X':d<-0.18?'T':null;
};
const DSA  = h=>{if(h.length<10)return null;const a=_avg(h.slice(-15).map(r=>r.total));return a<9.5?'T':a>11.5?'X':null;};
const DSV  = h=>{
    if(h.length<10) return null;
    const s=h.slice(-10).map(r=>r.total), a=_avg(s);
    const v=_avg(s.map(x=>Math.pow(x-a,2)));
    return v<2.0?(a<10.5?'X':'T'):null;
};
const DST  = h=>{
    if(h.length<10) return null;
    const s=h.slice(-10).map(r=>r.total);
    const tr=s.slice(-3).reduce((a,v,i,ar)=>i>0?a+(v-ar[i-1]):a,0)/2;
    return tr>1.5?'T':tr<-1.5?'X':null;
};
const RSI  = h=>{
    if(h.length<15) return null;
    const tx=h.map(r=>r.tx).slice(-14);
    let g=0,l=0;
    for(let i=1;i<tx.length;i++){if(tx[i]==='T'&&tx[i-1]==='X')g++;else if(tx[i]==='X'&&tx[i-1]==='T')l++;}
    const rsi=l===0?100:100-(100/(1+(g/l)));
    return rsi>70?'X':rsi<30?'T':null;
};
const WAVE = h=>{
    if(h.length<20) return null;
    const tx=h.map(r=>r.tx);
    for(const w of[5,8,10,15]){
        if(tx.length<w) continue;
        const T=tx.slice(-w).filter(c=>c==='T').length;
        if(T>=w*0.8) return 'X';
        if(T<=w*0.2) return 'T';
    }
    return null;
};
const CYC  = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<14) return null;
    for(const c of[3,5,7,9,11]){
        if(tx.length<c*2) continue;
        const rec=tx.slice(-c), prv=tx.slice(-c*2,-c);
        const m=rec.filter((v,i)=>v===prv[i]).length;
        if(m>=c-1&&tx.at(-(c+1))) return tx.at(-(c+1));
    }
    return null;
};

// ─── NHÓM ULTRA PATTERN RECOGNITION ────────────────────────
const PATTERN_DB = {
    'bet2':  [['T','T'],['X','X']],
    'bet3':  [['T','T','T'],['X','X','X']],
    'bet4':  [['T','T','T','T'],['X','X','X','X']],
    'bet5':  [['T','T','T','T','T'],['X','X','X','X','X']],
    'alt2':  [['T','X'],['X','T']],
    'alt3':  [['T','X','T'],['X','T','X']],
    'alt4':  [['T','X','T','X'],['X','T','X','T']],
    'alt5':  [['T','X','T','X','T'],['X','T','X','T','X']],
    'c22':   [['T','T','X','X'],['X','X','T','T']],
    'c33':   [['T','T','T','X','X','X'],['X','X','X','T','T','T']],
    'c44':   [['T','T','T','T','X','X','X','X'],['X','X','X','X','T','T','T','T']],
    'c221':  [['T','T','X','X','T'],['X','X','T','T','X']],
    'c122':  [['T','X','X','T','T'],['X','T','T','X','X']],
    'c212':  [['T','T','X','T','T'],['X','X','T','X','X']],
    'c121':  [['T','X','T','X','T'],['X','T','X','T','X']],
    'c312':  [['T','T','T','X','T','T'],['X','X','X','T','X','X']],
    'c213':  [['T','T','X','T','T','T'],['X','X','T','X','X','X']],
    'c321':  [['T','T','T','X','X','T'],['X','X','X','T','T','X']],
    'c123':  [['T','X','X','T','T','T'],['X','T','T','X','X','X']],
    'sym4':  [['T','X','X','T'],['X','T','T','X']],
    'sym6':  [['T','T','X','X','T','T'],['X','X','T','T','X','X']],
    'spi1':  [['T','X','X','T','X'],['X','T','T','X','T']],
    'spi2':  [['T','T','X','T','X','X'],['X','X','T','X','T','T']],
    'wave2': [['T','T','X','X','T','T'],['X','X','T','T','X','X']],
    'wave3': [['T','T','T','X','X','X','T','T','T'],['X','X','X','T','T','T','X','X','X']],
    'mix1':  [['T','T','X','T','X','T'],['X','X','T','X','T','X']],
    'mix2':  [['T','X','T','T','T','X'],['X','T','X','X','X','T']],
    'mix3':  [['T','T','X','X','T','X','T'],['X','X','T','T','X','T','X']],
    'deep1': [['T','X','T','T','X','X','T'],['X','T','X','X','T','T','X']],
    'deep2': [['T','T','X','T','X','X','T','X'],['X','X','T','X','T','T','X','T']],
};

const ULTRA_PAT = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<15) return null;
    let bScore=0, bPred=null;
    for(const[,variants] of Object.entries(PATTERN_DB)){
        for(const variant of variants){
            const vl=variant.length; if(tx.length<vl+3) continue;
            let T=0,X=0;
            for(let i=0;i<=tx.length-vl-1;i++){
                if(variant.every((c,j)=>c===tx[i+j])){tx[i+vl]==='T'?T++:X++;}
            }
            const s=Math.abs(T-X);
            if(s>bScore&&T+X>=2){bScore=s;bPred=T>X?'T':'X';}
        }
    }
    return bScore>=2?bPred:null;
};

// ─── NHÓM AI ENGINES (20 AI) ────────────────────────────────
const AI01 = h=>{for(let o=9;o>=1;o--){const p=mxP(h.map(r=>r.tx),o,2,0.57);if(p)return p;}return null;};

const AI02 = h=>{
    if(h.length<20) return null;
    const ps=[M1,M2,M3,NG3,NG4,SK34,FR_10].map(f=>f(h)).filter(Boolean);
    if(!ps.length) return null;
    const T=ps.filter(v=>v==='T').length, X=ps.filter(v=>v==='X').length;
    return T>X?'T':X>T?'X':null;
};

const AI03 = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<40) return null;
    const tgt=tx.slice(-10).join(''); let T=0,X=0;
    for(let i=0;i<=tx.length-11;i++){
        const s=_sim(tx.slice(i,i+10).join(''),tgt);
        if(s>=0.75){tx[i+10]==='T'?T+=s:X+=s;}
    }
    return T===X?null:T>X?'T':'X';
};

const AI04 = h=>CYC(h);

const AI05 = h=>{
    if(h.length<15) return null;
    let p=0.5;
    for(const r of h.map(x=>x.tx)){p=r==='T'?Math.min(0.95,p+0.03):Math.max(0.05,p-0.03);}
    return p>0.62?'X':p<0.38?'T':null;
};

const AI06 = h=>{
    const{runs}=extractFeat(h); if(runs.length<5) return null;
    const last=runs.at(-1), a=_avg(runs.slice(-5).map(r=>r.len));
    return last.len>a*1.6?(last.val==='T'?'X':'T'):last.len<a*0.5?last.val:null;
};

const AI07 = h=>{
    const tx=h.map(r=>r.tx); if(tx.length<100) return null;
    const tgt=tx.slice(-12).join(''); let T=0,X=0,W=0;
    for(let i=0;i<=tx.length-13;i++){
        const s=_sim(tx.slice(i,i+12).join(''),tgt);
        if(s>0.7){const w=s/(tx.length-i);tx[i+12]==='T'?T+=w:X+=w;W+=w;}
    }
    return W<0.5?null:T>X?'T':'X';
};

const AI08 = h=>{
    if(h.length<40) return null;
    const ps=[M1,M2,M3,M4,NG3,NG4,SK34,D22,FR_GL,FR_20,FR_10,REG,MOM,BAY,CHI2,RSI,HC,WAVE]
        .map(f=>f(h)).filter(Boolean);
    if(ps.length<5) return null;
    const T=ps.filter(v=>v==='T').length, X=ps.filter(v=>v==='X').length;
    return T/ps.length>=0.65?'T':X/ps.length>=0.65?'X':null;
};

const AI09 = h=>ULTRA_PAT(h);

const AI10 = h=>{
    if(h.length<20) return null;
    const s=h.slice(-20).map(r=>r.total), a=_avg(s), sd=_std(s), l=h.at(-1).total;
    return l>a+sd*1.2?'X':l<a-sd*1.2?'T':null;
};

const AI11 = h=>FR_MW(h);

const AI12 = h=>{
    const tx=h.map(r=>r.tx);
    const{val,len}=getStreakInfo(tx);
    if(len>=10) return val==='T'?'X':'T';
    if(len>=6){const{runs}=extractFeat(h);const a=_avg(runs.map(r=>r.len));if(len>a*2)return val==='T'?'X':'T';}
    return null;
};

const AI13 = h=>{
    const tx=h.map(r=>r.tx); let a=1;
    for(let i=tx.length-2;i>=0&&tx[i]!==tx[i+1];i--) a++;
    return a>=10?tx.at(-1):a>=6?(tx.at(-1)==='T'?'X':'T'):null;
};

const AI14 = h=>{
    if(h.length<25) return null;
    const tx=h.map(r=>r.tx).slice(-25), e=_ent(tx), T=tx.filter(c=>c==='T').length;
    return e<0.75?(T>12?'T':'X'):e>0.99?(tx.at(-1)==='T'?'X':'T'):null;
};

const AI15 = h=>CHI2(h);

const AI16 = h=>{
    const tx=h.map(r=>r.tx);
    for(let l=5;l>=2;l--){
        const k=tx.slice(-l).join(''), m=adaptiveMem[k];
        if(m&&(m.T+m.X)>=3){return m.T>m.X?'T':m.X>m.T?'X':null;}
    }
    return null;
};

const AI17 = h=>D22(h)||D33(h)||D44(h);

const AI18 = h=>WAVE(h);

const AI19 = h=>{
    if(h.length<15) return null;
    const b=h.map(r=>r.tx).map(v=>v==='T'?1:-1);
    const m3=_avg(b.slice(-3)), m7=_avg(b.slice(-7)), m15=_avg(b.slice(-15));
    if(m3>0.3&&m7>0&&m15<0) return 'T';
    if(m3<-0.3&&m7<0&&m15>0) return 'X';
    return m3>0.55?'T':m3<-0.55?'X':null;
};

const AI20 = h=>{
    if(h.length<50) return null;
    const ps=[AI01,AI02,AI03,AI05,AI06,AI08,AI09,AI11,AI12,AI14,AI15,AI17,AI18,AI19,
              M1,M2,M3,NG3,NG4,FR_GL,FR_20,REG,BAY].map(f=>f(h)).filter(Boolean);
    if(ps.length<7) return null;
    const T=ps.filter(v=>v==='T').length, X=ps.filter(v=>v==='X').length;
    return T/ps.length>=0.62?'T':X/ps.length>=0.62?'X':null;
};

// ─── NHÓM EXTENDED (50) ─────────────────────────────────────
const E01=h=>{const tx=h.map(r=>r.tx);if(tx.length<5)return null;const l=tx.slice(-5);return l.slice(0,4).every(v=>v===l[0])&&l[4]!==l[0]?l[0]:null;};
const E02=h=>{const tx=h.map(r=>r.tx);if(tx.length<12)return null;const p=tx.slice(-4).join(''),q=tx.slice(-8,-4).join(''),r=tx.slice(-12,-8).join('');return p===q&&q===r?(tx.at(-1)==='T'?'X':'T'):null;};
const E03=h=>{if(h.length<15)return null;const tx=h.map(r=>r.tx).slice(-15);const T=tx.filter(v=>v==='T').length;return T>=12?'X':T<=3?'T':null;};
const E04=h=>{if(h.length<20)return null;const tx=h.map(r=>r.tx).slice(-20);const ch=tx.filter((v,i)=>i>0&&v!==tx[i-1]).length;return ch>=17?(tx.at(-1)==='T'?'X':'T'):ch<=3?tx.at(-1):null;};
const E05=h=>{const{runs}=extractFeat(h);if(runs.length<4)return null;const ls=runs.slice(-4).map(r=>r.len);const inc=ls.every((v,i)=>i===0||v>=ls[i-1]);const dec=ls.every((v,i)=>i===0||v<=ls[i-1]);return inc?runs.at(-1).val:dec?(runs.at(-1).val==='T'?'X':'T'):null;};
const E06=h=>{if(h.length<10)return null;const s=h.slice(-10).map(r=>r.total),inc=s.every((v,i)=>i===0||v>=s[i-1]),dec=s.every((v,i)=>i===0||v<=s[i-1]);return inc?'T':dec?'X':null;};
const E07=h=>{if(h.length<30)return null;const tx=h.map(r=>r.tx).slice(-30);const cp=tx.filter((v,i)=>i>0&&v===tx[i-1]).length;return cp>=22?tx.at(-1):cp<=7?(tx.at(-1)==='T'?'X':'T'):null;};
const E08=h=>{const{meanTotal,stdTotal}=extractFeat(h);if(h.length<20)return null;const l=h.at(-1).total;return l>meanTotal+stdTotal*1.5?'X':l<meanTotal-stdTotal*1.5?'T':null;};
const E09=h=>{const fibs=new Set([1,2,3,5,8,13,21,34,55,89,144,233,377]);return fibs.has(h.length%377)&&h.length>1?(h.map(r=>r.tx).at(-1)==='T'?'X':'T'):null;};
const E10=h=>{if(h.length<10)return null;const s=h.slice(-10).map(r=>r.total),diffs=s.slice(1).map((v,i)=>v-s[i]);const alt=diffs.every((v,i)=>i===0||(v>0)!==(diffs[i-1]>0));return alt?(h.at(-1).tx==='T'?'X':'T'):null;};
const E11=h=>{const p=AI01(h),q=NG3(h);return p&&q&&p===q?p:null;};
const E12=h=>{const p=AI02(h),q=SK34(h);return p&&q&&p===q?p:null;};
const E13=h=>{const p=M3(h),q=REG(h);return p&&q&&p===q?p:null;};
const E14=h=>{const p=AI05(h),q=FR_GL(h);return p&&q&&p===q?p:null;};
const E15=h=>{const p=AI06(h),q=SK34(h);return p&&q&&p===q?p:null;};
const E16=h=>{if(h.length<20)return null;const tx=h.map(r=>r.tx).slice(-20);const T=tx.filter(c=>c==='T').length;return Math.abs(T-10)<=1?(tx.at(-1)==='T'?'X':'T'):null;};
const E17=h=>{const tx=h.map(r=>r.tx);if(tx.length<8)return null;const[a,b,c,d,e,f,g,k]=tx.slice(-8);return(a===b&&c===d&&e===f&&g===k&&a!==c&&c!==e&&e!==g)?g:null;};
const E18=h=>{if(h.length<25)return null;const{entropy:ent,runs}=extractFeat(h.slice(-25));return(ent<0.65&&(runs.at(-1)?.len||0)>=4)?runs.at(-1).val:null;};
const E19=h=>{if(h.length<5)return null;const l=h.at(-1),p=h.at(-2);return(l&&p&&l.dice.includes(6)&&p.dice.includes(6))?'T':null;};
const E20=h=>{if(h.length<5)return null;const l=h.at(-1),p=h.at(-2);return(l&&p&&l.dice.includes(1)&&p.dice.includes(1))?'X':null;};
const E21=h=>{const p=P4(h),b=AI05(h);return p&&b&&p===b?p:null;};
const E22=h=>{const p=P6(h),m=M3(h);return p&&m&&p===m?p:null;};
const E23=h=>{const p=WAVE(h),f=FR_MW(h);return p&&f&&p===f?p:null;};
const E24=h=>{const p=CYC(h),m=M4(h);return p&&m&&p===m?p:null;};
const E25=h=>{const p=MOM(h),q=MAC(h);return p&&q&&p===q?p:null;};
const E26=h=>{if(h.length<15)return null;const tx=h.map(r=>r.tx);const T=tx.slice(-5).filter(v=>v==='T').length,last=tx.at(-1);return T>=4&&last==='T'?'X':T<=1&&last==='X'?'T':null;};
const E27=h=>{const ps=[M1,M2,M3,NG3,NG4,M2W].map(f=>f(h)).filter(Boolean);if(ps.length<4)return null;const T=ps.filter(v=>v==='T').length;return T>=4?'T':T===0?'X':null;};
const E28=h=>{const ps=[SK2,SK34,SK56,C21,C12,D22,ALT4].map(f=>f(h)).filter(Boolean);if(ps.length<3)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T>X&&T/ps.length>=0.70?'T':X>T&&X/ps.length>=0.70?'X':null;};
const E29=h=>{const ps=[FR_GL,FR_20,FR_10,FR_MW,REG,HC].map(f=>f(h)).filter(Boolean);if(ps.length<3)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T>X&&T/ps.length>=0.65?'T':X>T&&X/ps.length>=0.65?'X':null;};
const E30=h=>{const ps=[AI17,AI18,AI19,E05,E06,E07].map(f=>f(h)).filter(Boolean);if(ps.length<2)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T>X?'T':X>T?'X':null;};
const E31=h=>{if(h.length<10)return null;const s=h.slice(-10).map(r=>r.total),pairs=s.filter(t=>t%2===0).length;return pairs>=8?'T':pairs<=2?'X':null;};
const E32=h=>{const ps=[E27,E28,E29,E30,AI08,AI20].map(f=>f(h)).filter(Boolean);if(ps.length<3)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T/ps.length>=0.65?'T':X/ps.length>=0.65?'X':null;};
const E33=h=>{const tx=h.map(r=>r.tx);if(tx.length<6)return null;const l6=tx.slice(-6).join('');return['TTTTTT','XXXXXX'].includes(l6)?(l6[0]==='T'?'X':'T'):null;};
const E34=h=>P5(h);
const E35=h=>P7(h);
const E36=h=>{const{runs}=extractFeat(h);if(runs.length<6)return null;const l6=runs.slice(-6);const avgL=_avg(l6.map(r=>r.len)),last=l6.at(-1);return last.len>avgL*2?(last.val==='T'?'X':'T'):last.len<avgL*0.3?last.val:null;};
const E37=h=>{if(h.length<20)return null;const tx=h.map(r=>r.tx).slice(-20);let T=0,X=0;tx.forEach((v,i)=>{(v==='T'?T:X)+=Math.log2(i+2);});return T>X*1.2?'T':X>T*1.2?'X':null;};
const E38=h=>{if(h.length<30)return null;const tx=h.map(r=>r.tx);const r30=tx.slice(-30).filter(c=>c==='T').length/30,r10=tx.slice(-10).filter(c=>c==='T').length/10,d=r10-r30;return Math.abs(d)>0.25?(d>0?'X':'T'):null;};
const E39=h=>{const tx=h.map(r=>r.tx);if(tx.length<8)return null;const[a,b,c,d,e,f,g,k]=tx.slice(-8);return(a===b&&b!==c&&c===d&&d!==e&&e===f&&f!==g&&g===k)?k:null;};
const E40=h=>{if(h.length<10)return null;const sums=h.slice(-10).map(r=>r.total),a=_avg(sums);const ext=sums.filter(s=>Math.abs(s-a)>3).length;return ext>=5?(h.at(-1).tx==='T'?'X':'T'):null;};
const E41=h=>{if(h.length<10)return null;const tx=h.map(r=>r.tx).slice(-10),last=tx.at(-1),same=tx.filter(v=>v===last).length;return same>=8?(last==='T'?'X':'T'):same<=2?last:null;};
const E42=h=>{const p=NG3W(h),q=M2W(h);return p&&q&&p===q?p:null;};
const E43=h=>{if(h.length<15)return null;const{maxRun}=extractFeat(h.slice(-15));return maxRun>=8?(h.map(r=>r.tx).at(-1)==='T'?'X':'T'):null;};
const E44=h=>{if(h.length<20)return null;const sums=h.slice(-20).map(r=>r.total);let peaks=0;for(let i=1;i<sums.length-1;i++){if(sums[i]>sums[i-1]&&sums[i]>sums[i+1])peaks++;}return peaks>=6?'X':peaks<=1?'T':null;};
const E45=h=>{const ps=[E33,E36,E37,E38,E39,E40,E41,E43,E44].map(f=>f(h)).filter(Boolean);if(ps.length<3)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T>X?'T':X>T?'X':null;};
const E46=h=>{const p=C31(h),q=M3(h);return p&&q&&p===q?p:null;};
const E47=h=>{const p=C13(h),q=NG4(h);return p&&q&&p===q?p:null;};
const E48=h=>{const p=SK7(h),q=FR_20(h);return p&&q&&p===q?p:null;};
const E49=h=>{if(h.length<20)return null;const ps=[M9,M8,M7,M6,M5].map(f=>f(h)).filter(Boolean);if(!ps.length)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T>X?'T':X>T?'X':null;};
const E50=h=>{const ps=[E45,E46,E47,E48,E49,E32,AI20].map(f=>f(h)).filter(Boolean);if(ps.length<3)return null;const T=ps.filter(v=>v==='T').length,X=ps.filter(v=>v==='X').length;return T/ps.length>=0.62?'T':X/ps.length>=0.62?'X':null;};
const E51=h=>P8(h);
const E52=h=>{const p=AI09(h),q=P4(h);return p&&q&&p===q?p:null;};
const E53=h=>{const p=D33(h),q=M3(h);return p&&q&&p===q?p:null;};
const E54=h=>{const p=RSI(h),q=BAY(h);return p&&q&&p===q?p:null;};
const E55=h=>{if(h.length<10)return null;const tx=h.map(r=>r.tx).slice(-10);const T=tx.filter(v=>v==='T').length;return T===5?(tx.at(-1)==='T'?'X':'T'):null;};

// ============================================================
// DANH SÁCH 120 THUẬT TOÁN
// ============================================================
const ALL_ALGOS = [
    // Markov 12
    {id:'M1',fn:M1,w:1.0},{id:'M2',fn:M2,w:1.1},{id:'M3',fn:M3,w:1.2},
    {id:'M4',fn:M4,w:1.2},{id:'M5',fn:M5,w:1.1},{id:'M6',fn:M6,w:1.0},
    {id:'M7',fn:M7,w:0.9},{id:'M8',fn:M8,w:0.8},{id:'M9',fn:M9,w:0.7},
    {id:'M2W',fn:M2W,w:1.0},{id:'M3W',fn:M3W,w:1.0},{id:'M4W',fn:M4W,w:1.0},
    // N-gram 10
    {id:'NG3',fn:NG3,w:1.1},{id:'NG4',fn:NG4,w:1.1},{id:'NG5',fn:NG5,w:1.0},
    {id:'NG6',fn:NG6,w:0.9},{id:'NG7',fn:NG7,w:0.8},{id:'NG3W',fn:NG3W,w:1.0},
    {id:'NG4W',fn:NG4W,w:1.0},{id:'NGFz',fn:NGFz,w:1.0},{id:'NGF8',fn:NGF8,w:0.9},
    // Streak 14
    {id:'SK2',fn:SK2,w:0.9},{id:'SK34',fn:SK34,w:1.1},{id:'SK56',fn:SK56,w:1.1},
    {id:'SK7',fn:SK7,w:1.2},{id:'SK10',fn:SK10,w:1.3},{id:'ALT4',fn:ALT4,w:1.1},
    {id:'ALT8',fn:ALT8,w:0.9},{id:'D22',fn:D22,w:1.1},{id:'D33',fn:D33,w:1.0},
    {id:'D44',fn:D44,w:1.0},{id:'C21',fn:C21,w:1.0},{id:'C12',fn:C12,w:1.0},
    {id:'C31',fn:C31,w:1.0},{id:'C13',fn:C13,w:1.0},
    // Pattern map 8
    {id:'P4',fn:P4,w:1.0},{id:'P5',fn:P5,w:1.0},{id:'P6',fn:P6,w:1.0},
    {id:'P7',fn:P7,w:0.9},{id:'P8',fn:P8,w:0.9},{id:'C41',fn:C41,w:1.0},
    {id:'C14',fn:C14,w:1.0},{id:'UPAT',fn:ULTRA_PAT,w:1.3},
    // Thống kê 16
    {id:'FR_GL',fn:FR_GL,w:1.0},{id:'FR_20',fn:FR_20,w:1.1},{id:'FR_10',fn:FR_10,w:1.0},
    {id:'FR_MW',fn:FR_MW,w:1.2},{id:'REG',fn:REG,w:1.0},{id:'MOM',fn:MOM,w:1.1},
    {id:'MAC',fn:MAC,w:1.0},{id:'BAY',fn:BAY,w:1.3},{id:'ENT',fn:ENT,w:0.9},
    {id:'CHI2',fn:CHI2,w:1.0},{id:'HC',fn:HC,w:1.0},{id:'DSA',fn:DSA,w:0.8},
    {id:'DSV',fn:DSV,w:0.7},{id:'DST',fn:DST,w:0.8},{id:'RSI',fn:RSI,w:1.0},
    {id:'WAVE',fn:WAVE,w:1.0},
    // Misc 2
    {id:'CYC',fn:CYC,w:0.9},{id:'REG2',fn:h=>REG(h),w:0.9},
    // AI 20
    {id:'AI01',fn:AI01,w:1.5},{id:'AI02',fn:AI02,w:1.4},{id:'AI03',fn:AI03,w:1.3},
    {id:'AI04',fn:AI04,w:1.1},{id:'AI05',fn:AI05,w:1.3},{id:'AI06',fn:AI06,w:1.1},
    {id:'AI07',fn:AI07,w:1.2},{id:'AI08',fn:AI08,w:1.5},{id:'AI09',fn:AI09,w:1.4},
    {id:'AI10',fn:AI10,w:1.0},{id:'AI11',fn:AI11,w:1.2},{id:'AI12',fn:AI12,w:1.2},
    {id:'AI13',fn:AI13,w:1.1},{id:'AI14',fn:AI14,w:1.0},{id:'AI15',fn:AI15,w:1.0},
    {id:'AI16',fn:AI16,w:1.4},{id:'AI17',fn:AI17,w:1.1},{id:'AI18',fn:AI18,w:1.0},
    {id:'AI19',fn:AI19,w:1.1},{id:'AI20',fn:AI20,w:1.8},
    // Extended 55
    {id:'E01',fn:E01,w:1.0},{id:'E02',fn:E02,w:1.0},{id:'E03',fn:E03,w:1.0},
    {id:'E04',fn:E04,w:1.0},{id:'E05',fn:E05,w:1.0},{id:'E06',fn:E06,w:0.8},
    {id:'E07',fn:E07,w:1.0},{id:'E08',fn:E08,w:0.9},{id:'E09',fn:E09,w:0.7},
    {id:'E10',fn:E10,w:0.8},{id:'E11',fn:E11,w:1.2},{id:'E12',fn:E12,w:1.1},
    {id:'E13',fn:E13,w:1.1},{id:'E14',fn:E14,w:1.1},{id:'E15',fn:E15,w:1.1},
    {id:'E16',fn:E16,w:0.9},{id:'E17',fn:E17,w:1.0},{id:'E18',fn:E18,w:1.0},
    {id:'E19',fn:E19,w:0.7},{id:'E20',fn:E20,w:0.7},{id:'E21',fn:E21,w:1.1},
    {id:'E22',fn:E22,w:1.1},{id:'E23',fn:E23,w:1.0},{id:'E24',fn:E24,w:1.0},
    {id:'E25',fn:E25,w:1.0},{id:'E26',fn:E26,w:1.0},{id:'E27',fn:E27,w:1.2},
    {id:'E28',fn:E28,w:1.2},{id:'E29',fn:E29,w:1.1},{id:'E30',fn:E30,w:1.0},
    {id:'E31',fn:E31,w:0.8},{id:'E32',fn:E32,w:1.2},{id:'E33',fn:E33,w:1.0},
    {id:'E34',fn:E34,w:1.0},{id:'E35',fn:E35,w:1.0},{id:'E36',fn:E36,w:1.0},
    {id:'E37',fn:E37,w:1.0},{id:'E38',fn:E38,w:1.0},{id:'E39',fn:E39,w:1.0},
    {id:'E40',fn:E40,w:0.9},{id:'E41',fn:E41,w:1.0},{id:'E42',fn:E42,w:1.1},
    {id:'E43',fn:E43,w:0.9},{id:'E44',fn:E44,w:0.9},{id:'E45',fn:E45,w:1.1},
    {id:'E46',fn:E46,w:1.1},{id:'E47',fn:E47,w:1.1},{id:'E48',fn:E48,w:1.1},
    {id:'E49',fn:E49,w:1.0},{id:'E50',fn:E50,w:1.2},{id:'E51',fn:E51,w:0.9},
    {id:'E52',fn:E52,w:1.0},{id:'E53',fn:E53,w:1.0},{id:'E54',fn:E54,w:1.0},
    {id:'E55',fn:E55,w:0.8},
];

// ============================================================
// MASTER AI ENGINE
// ============================================================
class MasterAI {
    constructor() {
        this.history  = [];
        this.weights  = {};
        this.perf     = {};
        this.breakMgr = new BreakCauManager();
        this.lastRaw  = null;
        this.ema      = 0.07;
        this.minW     = 0.001;
        for (const a of ALL_ALGOS) {
            this.weights[a.id] = a.w;
            this.perf[a.id]    = { win:0, lose:0 };
        }
        this._norm();
    }

    _norm() {
        const s = Object.values(this.weights).reduce((a,b)=>a+b,0) || 1;
        for (const id in this.weights) this.weights[id] /= s;
    }

    fitInitial() {
        const win = _lastN(this.history, 500);
        if (win.length < 15) return;
        const sc = {};
        for (const a of ALL_ALGOS) sc[a.id] = 0;
        for (let i=15; i<win.length; i++) {
            const pre = win.slice(0,i), act = win[i].tx;
            for (const a of ALL_ALGOS) {
                try { const p=a.fn(pre); if(p&&p===act) sc[a.id]++; } catch {}
            }
        }
        let tot = 0;
        for (const a of ALL_ALGOS) {
            const bn = (sc[a.id]||0) / Math.max(win.length-15,1);
            this.weights[a.id] = Math.max(this.minW, a.w + bn*0.5);
            tot += this.weights[a.id];
        }
        for (const id in this.weights) this.weights[id] /= tot;
        console.log(`✅ fit ${ALL_ALGOS.length} algos trên ${win.length} phiên`);
    }

    _selfLearnUpdate(history, actual) {
        for (const a of ALL_ALGOS) {
            try {
                const p = a.fn(history);
                if (!p) continue;
                const ok = p===actual;
                const rw = ok ? 1.07 : 0.93;
                const tg = this.weights[a.id] * rw;
                this.weights[a.id] = Math.max(this.minW,
                    this.ema*tg + (1-this.ema)*this.weights[a.id]);
                ok ? this.perf[a.id].win++ : this.perf[a.id].lose++;
            } catch {}
        }
        // Adaptive memory update
        const tx = history.map(r=>r.tx);
        for (let l=5; l>=2; l--) {
            const k = tx.slice(-l).join('');
            adaptiveMem[k] = adaptiveMem[k] || {T:0,X:0};
            adaptiveMem[k][actual]++;
        }
        // Bẻ cầu update
        if (this.lastRaw) this.breakMgr.update(this.lastRaw, actual);
        this._norm();
    }

    predict() {
        if (this.history.length < 3) {
            return { raw:'T', final:'T', pred:'tài', conf:0.50, models:0, phase:'bootstrap',
                voteT:0, voteX:0, wasFlipped:false, breakStatus:this.breakMgr.status() };
        }
        const votes = {T:0, X:0};
        let fired=0, totalW=0;
        for (const a of ALL_ALGOS) {
            try {
                const p = a.fn(this.history);
                if (!p) continue;
                const w = this.weights[a.id] || a.w;
                votes[p] += w; totalW += w; fired++;
            } catch {}
        }
        let raw, conf;
        if (!totalW) {
            const T15 = this.history.map(r=>r.tx).slice(-15).filter(v=>v==='T').length;
            raw = T15>=8?'X':'T'; conf = 0.52;
        } else {
            raw  = votes.T >= votes.X ? 'T' : 'X';
            conf = Math.min(0.96, Math.max(0.52, Math.max(votes.T,votes.X)/totalW));
        }
        this.lastRaw = raw;
        const final  = this.breakMgr.apply(raw);
        return {
            raw, final,
            pred:        final==='T'?'tài':'xỉu',
            conf:        parseFloat(conf.toFixed(4)),
            models:      fired,
            totalAlgos:  ALL_ALGOS.length,
            wasFlipped:  final!==raw,
            voteT:       parseFloat(votes.T.toFixed(4)),
            voteX:       parseFloat(votes.X.toFixed(4)),
            breakStatus: this.breakMgr.status(),
            phase:       this.history.length<20?'learning':'full'
        };
    }

    // Thêm kết quả mới + update toàn bộ
    addRecord(item) {
        const rec    = parseRecord(item);
        const prefix = [...this.history];

        this.history.push(rec);
        if (this.history.length > 1000) this.history.shift();

        // Cập nhật pattern_10
        pattern10.push(rec.tx);
        if (pattern10.length > 10) pattern10.shift();

        // Self-learning update (cần ít nhất 3 phiên)
        if (prefix.length >= 3) this._selfLearnUpdate(prefix, rec.tx);

        // Lưu lịch sử dự đoán đúng/sai
        if (pendingPred && prefix.length >= 1) {
            const prev       = prefix.at(-1);
            const trang_thai = rec.tx === pendingPred.rawPred
                ? 'AUDU LẠI BÚ✅'
                : 'DITMEMAY LẠI GÃY';
            dudoanHistory.unshift({
                phien_truoc:  prev.session,
                result:       rec.tx,
                phien_nay:    rec.session,
                du_doan:      pendingPred.rawPred,
                do_tin_cay:   `${(pendingPred.conf*100).toFixed(0)}%`,
                trang_thai,
                time:         new Date().toISOString()
            });
            if (dudoanHistory.length > 1000) dudoanHistory.pop();
        }

        // Tính dự đoán tiếp → lưu làm pendingPred
        const nextP  = this.predict();
        pendingPred  = { rawPred: nextP.final, conf: nextP.conf, phien: rec.session };

        return { rec, nextP };
    }

    loadHistory(lines) {
        this.history  = lines.map(parseRecord).sort((a,b)=>a.session-b.session);
        pattern10     = this.history.slice(-10).map(r=>r.tx);
        dudoanHistory = [];   // reset khi load lịch sử mới
        pendingPred   = null;
        this.fitInitial();
    }

    getPattern() {
        if (this.history.length < 10) return {
            recent:'đang thu thập...', long:'đang thu thập...', discovered:'n/a' };
        const tx = this.history.map(h=>h.tx);
        const str30 = tx.slice(-30).join('').toLowerCase();
        let dom=null, maxOcc=0;
        for (const[,variants] of Object.entries(PATTERN_DB)) {
            for (const v of variants) {
                let cnt=0;
                for(let i=0;i<=str30.length-v.length;i++){
                    if(str30.slice(i,i+v.length)===v.join('').toLowerCase()) cnt++;
                }
                if(cnt>maxOcc){maxOcc=cnt;dom=v.join('');}
            }
        }
        return {
            recent:     tx.slice(-20).join('').toLowerCase(),
            long:       tx.slice(-50).join('').toLowerCase(),
            discovered: dom||'n/a',
            entropy_20: _ent(tx.slice(-20)).toFixed(4)
        };
    }

    getStats() {
        return ALL_ALGOS.slice(0,20).map(a=>{
            const p=this.perf[a.id], tot=p.win+p.lose;
            return { id:a.id, weight:this.weights[a.id].toFixed(5),
                acc: tot>0?`${(p.win/tot*100).toFixed(1)}%`:'N/A', total:tot };
        }).sort((a,b)=>parseFloat(b.weight)-parseFloat(a.weight));
    }
}

// ============================================================
// KHỞI TẠO AI
// ============================================================
const ai = new MasterAI();

// ============================================================
// FASTIFY SERVER
// ============================================================
const app = fastify({ logger: false });
await app.register(cors, { origin: '*' });

// ─── /api/taixiu/sunwin ─────────────────────────────────────
app.get('/api/taixiu/sunwin', async () => {
    try {
        const valid   = rikResults.filter(r => r.dice?.length===3);
        const last    = valid[0] || null;
        const p       = ai.predict();
        const pattern = ai.getPattern();
        const p10     = pattern10.join('');

        if (!last) return {
            id: 'HOANG GAME NEW Sunwin AI Ultra v5.0',
            status: 'đang chờ dữ liệu...',
            pattern_10: p10,
            du_doan: 'chưa có'
        };
        return {
            id:           'HOANG GAME NEW Sunwin AI Ultra v5.0',
            phien_truoc:  last.session,
            xuc_xac:      last.dice,
            tong:         last.total,
            ket_qua:      last.result?.toLowerCase(),
            pattern_10:   p10,
            pattern_20:   pattern.recent,
            pattern_50:   pattern.long,
            pattern_chu_dao: pattern.discovered,
            entropy:      pattern.entropy_20,
            phien_hien_tai: last.session+1,
            du_doan:      p.pred,
            do_tin_cay:   `${(p.conf*100).toFixed(0)}%`,
            be_cau:       p.wasFlipped,
            active_models: p.models,
            total_algos:  p.totalAlgos,
            vote_tai:     p.voteT,
            vote_xiu:     p.voteX,
            phase:        p.phase,
            break_status: p.breakStatus
        };
    } catch(e) {
        return { error: e.message };
    }
});

// ─── /api/taixiu/history ────────────────────────────────────
app.get('/api/taixiu/history', async () => {
    const valid = rikResults.filter(r=>r.dice?.length===3);
    if (!valid.length) return { message:'chưa có dữ liệu' };
    return valid.slice(0,50).map(r=>({
        session: r.session,
        dice:    r.dice,
        total:   r.total,
        result:  r.result?.toLowerCase()||'n/a',
        tx:      r.total>=11?'T':'X'
    }));
});

// ─── /api/history/dudoan ────────────────────────────────────
app.get('/api/history/dudoan', async req => {
    const limit = Math.min(1000, parseInt(req.query.limit)||100);
    const data  = dudoanHistory.slice(0, limit);
    const dung  = data.filter(r=>r.trang_thai.includes('✅')).length;
    const sai   = data.filter(r=>r.trang_thai.includes('GÃY')).length;
    return {
        total:        data.length,
        dung,
        sai,
        ty_le_dung:   data.length ? `${(dung/data.length*100).toFixed(1)}%` : 'N/A',
        pattern_10:   pattern10.join(''),
        data
    };
});

// ─── /api/taixiu/predict ────────────────────────────────────
app.get('/api/taixiu/predict', async () => {
    const p = ai.predict();
    return {
        du_doan:    p.pred,
        do_tin_cay: `${(p.conf*100).toFixed(0)}%`,
        be_cau:     p.wasFlipped,
        pattern_10: pattern10.join(''),
        phase:      p.phase,
        models:     `${p.models}/${p.totalAlgos}`
    };
});

// ─── /api/taixiu/weights ────────────────────────────────────
app.get('/api/taixiu/weights', async () => ({
    top15:       ai.getStats(),
    total_algos: ALL_ALGOS.length,
    break_log:   ai.breakMgr.log.slice(-10),
    break_status:ai.breakMgr.status()
}));

// ─── / ──────────────────────────────────────────────────────
app.get('/', async () => ({
    status:  'online',
    version: 'TX Ultra AI v5.0 — 120 Algos + 20 AI',
    endpoints: {
        predict:  '/api/taixiu/sunwin',
        history:  '/api/taixiu/history',
        dudoan:   '/api/history/dudoan',
        weights:  '/api/taixiu/weights',
        quick:    '/api/taixiu/predict'
    }
}));

// ============================================================
// WEBSOCKET
// ============================================================
function decodeBinaryMessage(data) {
    try {
        const msg = new TextDecoder().decode(data);
        if (msg.startsWith('[')||msg.startsWith('{')) return JSON.parse(msg);
        return null;
    } catch { return null; }
}
function sendCmd1005() {
    if (rikWS?.readyState===WebSocket.OPEN) {
        try { rikWS.send(JSON.stringify([6,'MiniGame','taixiuPlugin',{cmd:1005}])); } catch {}
    }
}

function connectWS() {
    console.log('\n🔌 Đang kết nối WebSocket...');
    if (rikWS&&(rikWS.readyState===WebSocket.OPEN||rikWS.readyState===WebSocket.CONNECTING)) rikWS.close();
    clearInterval(rikIntervalCmd);
    try { rikWS = new WebSocket(`${WS_URL}${TOKEN}`); }
    catch(e) { console.error('WS create error:',e.message); setTimeout(connectWS,5000); return; }

    rikWS.on('open', ()=>{
        console.log('✅ WebSocket connected');
        const auth = [1,'MiniGame','SC_giathinh2133','thinh211',{
            info: JSON.stringify({
                ipAddress: '2402:800:62cd:b4d1:8c64:a3c9:12bf:c19a',
                wsToken:   TOKEN,
                userId:    'cdbaf598-e4ef-47f8-b4a6-a4881098db86',
                username:  'SC_hellokietne212',
                timestamp: Date.now()
            }),
            signature: '473ABDDDA6BDD74D8F0B6036223B0E3A002A518203A9BB9F95AD763E3BF969EC2CBBA61ED1A3A9E217B52A4055658D7BEA38F89B806285974C7F3F62A9400066709B4746585887D00C9796552671894F826E69EFD234F6778A5DDC24830CEF68D51217EF047644E0B0EB1CB26942EB34AEF114AEC36A6DF833BB10F7D122EA5E',
            pid: 5, subi: true
        }];
        try { rikWS.send(JSON.stringify(auth)); } catch {}
        rikIntervalCmd = setInterval(sendCmd1005, 5000);
    });

    rikWS.on('message', data=>{
        try {
            const json = typeof data==='string' ? JSON.parse(data) : decodeBinaryMessage(data);
            if (!json) return;

            // Phiên mới realtime
            if (json.session && Array.isArray(json.dice)) {
                const {rec, nextP} = ai.addRecord(json);
                if (!rikCurrentSession || json.session > rikCurrentSession) {
                    rikCurrentSession = json.session;
                    rikResults.unshift(rec);
                    if (rikResults.length>200) rikResults.pop();
                }
                const dh = dudoanHistory[0];
                console.log(`\n${'='.repeat(55)}`);
                console.log(`📥 Phiên ${rec.session}: ${rec.tx} (${rec.total})`);
                console.log(`📟 Pattern10: [${pattern10.join('')}]`);
                console.log(`🔮 Dự đoán ${rec.session+1}: ${nextP.pred.toUpperCase()} (${(nextP.conf*100).toFixed(0)}%)${nextP.wasFlipped?' [BẺ CẦU]':''}`);
                console.log(`🤖 Models: ${nextP.models}/${nextP.totalAlgos} | Phase: ${nextP.phase}`);
                if (dh) console.log(`📋 Kết quả vừa: ${dh.trang_thai}`);
            }

            // Lịch sử từ server
            if (Array.isArray(json) && json[1]?.htr) {
                const lines = json[1].htr.map(i=>({
                    session: i.sid, dice:[i.d1,i.d2,i.d3],
                    total:   i.d1+i.d2+i.d3,
                    result:  i.d1+i.d2+i.d3>=11?'Tài':'Xỉu'
                })).sort((a,b)=>a.session-b.session);

                ai.loadHistory(lines);
                rikResults       = lines.slice(-100).sort((a,b)=>b.session-a.session).map(parseRecord);
                rikCurrentSession = lines.at(-1)?.session || null;

                const p     = ai.predict();
                pendingPred = { rawPred:p.final, conf:p.conf, phien:(rikCurrentSession||0)+1 };

                console.log(`\n${'='.repeat(55)}`);
                console.log(`📦 Đã tải ${lines.length} phiên lịch sử`);
                console.log(`🤖 Dự đoán tiếp: ${p.pred.toUpperCase()} (${(p.conf*100).toFixed(0)}%)${p.wasFlipped?' [BẺ CẦU]':''}`);
                console.log(`📟 Pattern10: [${pattern10.join('')}]`);
                console.log(`📊 Top 3 weights:`);
                ai.getStats().slice(0,3).forEach((s,i)=>{
                    console.log(`   ${i+1}. ${s.id} — W:${s.weight} | ACC:${s.acc}`);
                });
            }
        } catch(e) { console.error('WS parse error:',e.message); }
    });

    rikWS.on('close', ()=>{
        console.log('🔌 WS disconnected. Reconnect in 3s...');
        clearInterval(rikIntervalCmd);
        setTimeout(connectWS, 3000);
    });
    rikWS.on('error', err=>{ console.error('WS error:',err.message); rikWS.close(); });
}

// ============================================================
// START
// ============================================================
const startServer = async () => {
    try {
        await app.listen({ port:PORT, host:'0.0.0.0' });
        let ip = '0.0.0.0';
        try { ip=(await (await fetch('https://ifconfig.me/ip')).text()).trim(); } catch {}
        console.log('\n'+'='.repeat(65));
        console.log('  HOANG GAME NEW Sunwin AI ULTRA v5.0');
        console.log('  120 Thuật toán | 20 AI Engine | Bẻ Cầu Auto | Zero Random');
        console.log('='.repeat(65));
        console.log(`  📍 Local:   http://localhost:${PORT}/`);
        console.log(`  🌐 Network: http://${ip}:${PORT}/`);
        console.log(`  🔮 Predict: http://${ip}:${PORT}/api/taixiu/sunwin`);
        console.log(`  📜 History: http://${ip}:${PORT}/api/taixiu/history`);
        console.log(`  📋 Dudoan:  http://${ip}:${PORT}/api/history/dudoan`);
        console.log(`  ⚖️  Weights: http://${ip}:${PORT}/api/taixiu/weights`);
        console.log('='.repeat(65)+'\n');
    } catch(err) { console.error('❌ Server error:',err); process.exit(1); }
};

startServer().then(()=>connectWS()).catch(e=>{ console.error(e); process.exit(1); });
