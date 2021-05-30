package org.elasql.bench.server.procedure.tpart.ycsb;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;

public class TpartYcsbStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (YcsbTransactionType.fromProcedureId(pid)) {
			case YCSB:
				sp = new TpartYcsbProc(txNum);
				break;
			default:
				throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}
}
