package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import static java.lang.Math.round;

public class AdvancedProgressMeter {

    private final long _start;
    private long _preLast;
    private long _last;
    private long _count;
    private long _limit;

    public AdvancedProgressMeter(long limit) {
        _start = System.currentTimeMillis();
        _last = System.currentTimeMillis();
        _preLast = System.currentTimeMillis();
        _count = 0;
        _limit = limit;
    }

    public void next()
    {
        _count++;
        _preLast = _last;
        _last = System.currentTimeMillis();
    }

    public void setMax(long iMax){
        this._limit=iMax;
    }

    public long getCount()
    {
        return _count;
    }

    public void setDone(final long count)
    {
        _count = count;
        _preLast = _last;
        _last = System.currentTimeMillis();
    }

    public void setLeft(final long count)
    {
        _count = _limit - count;
        _preLast = _last;
        _last = System.currentTimeMillis();
    }

    public long getTimeSoFar()
    {
        return _last - _start;
    }

    public long getEstimatedTime()
    {
        return round(((double) (getTimeSoFar()) / _count) * _limit);
    }

    public long getRemainingTime()
    {
        return round(getEstimatedTime() - getTimeSoFar());
    }

    public int getPercentage()
    {
        return 100 - (int) (((_limit - _count) * 100) / _limit);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append(_count);
        sb.append(" of ");
        sb.append(_limit);
        if (_count > 0 && _limit > 0) {
            final int perc = 100 - (int) (((_limit - _count) * 100) / _limit);
            sb.append(" (");
            sb.append(perc);
            sb.append("%  ETA ");
            final double timeSoFar = (_last - _start);
            final long estTotal = round((timeSoFar / _count) * _limit);
            final long timeLeft = round(estTotal - timeSoFar);
            sb.append(milliToStringShort(timeLeft));
            sb.append("  RUN ");
            sb.append(milliToStringShort(_last - _start));
            sb.append("  AVG ");
            sb.append(round(timeSoFar / _count));
            sb.append("  LAST ");
            sb.append(_last - _preLast);
            sb.append(")");
        }
        return sb.toString();
    }

    public static String milliToStringShort(final long milli)
    {
        final long fracs = milli % 1000;
        final long seconds = milli / 1000;
        final long minutes = seconds / 60;
        final long hours = minutes / 60;
        final long days = hours / 24;
        return String.format("%02d:%02d:%02d:%02d.%-3d", (days % 365), (hours % 24), (minutes % 60), (seconds % 60), fracs);
    }
}
