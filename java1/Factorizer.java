/**
 * Paulina Lagebjer Kekkonen (pala7490)
 */

//
//Your task is to implement a concurrent and thread-safe program according to the specification
//above, and to make the program as performant as possible including the requirement that the
//execution of all threads should be properly terminated when the two factors have been found in one
//thread. The termination should be done as soon as reasonable to achieve the best overall
//performance.


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;


//TODO: @ThreadSafe
class Factorizer implements Runnable {
	private final static BigInteger MIN = new BigInteger("2"); //immutable shared state
	private static boolean foundAns = false; //mutable shared state, use lock when reading and writing
    private static final Object countLock = new Object();

	private BigInteger product;	
	private BigInteger min;
	private BigInteger max;
	private int steps;
	private BigInteger factor1;
	private BigInteger factor2;
	
	Factorizer(BigInteger min, BigInteger product, int steps) {
		this.min = min;
		this.product = product;
		this.steps = steps;
		max = product.subtract(BigInteger.ONE);
	}
	

	
	public void run() {
		BigInteger number = min;
		while (number.compareTo(max) <= 0) {
			synchronized (countLock) {
				if(!foundAns) {
					if (product.remainder(number).compareTo(BigInteger.ZERO) == 0) {
						factor1 = number;
						factor2 = product.divide(factor1);
						foundAns = true;
						return;
					}
					number = number.add(new BigInteger(Integer.toString(steps)));
				} else {
					return;
				}
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			// Read input.
			InputStreamReader streamReader = new InputStreamReader(System.in);
			BufferedReader consoleReader = new BufferedReader(streamReader);
			System.out.print("Input (numThreads)>");
			String inputThreads = consoleReader.readLine();
			int numThreads = Integer.parseInt(inputThreads);

			System.out.print("Input (product)>");
			String inputProduct = consoleReader.readLine();
			BigInteger p = new BigInteger(inputProduct);
			
			// Start timing.
			long start = System.nanoTime();
			
			// Create threads.
			Thread[] threads = new Thread[numThreads];
			Factorizer[] factorizers = new Factorizer[numThreads];			
			for (int i = 0; i < numThreads; i++) {
				factorizers[i] = new Factorizer(MIN.add(new BigInteger(Integer.toString(i))), p, numThreads);
				threads[i] = new Thread(factorizers[i]);
			}
			
			// Run threads.
			BigInteger f1 = null, f2 = null;
			for (int i = 0; i < numThreads; i++) {
				threads[i].start();
			}
			int i = 0;
			while(f1 == null && i < numThreads) {
				threads[i].join();
				if(factorizers[i].factor1 != null) {
					f1 = factorizers[i].factor1;
					f2 = factorizers[i].factor2;
				}
				i++;
			}

			// Stop timing.
			long stop = System.nanoTime();

			// Output results.
			int numProcessors = Runtime.getRuntime().availableProcessors();
			System.out.println("Range searched: " + MIN + " - " + p);
			String ans = f1 ==null? "No factorization possible" : "Factors: " + f1 + "*" + f2;
			System.out.println(ans);
			System.out.println("Available processors: " + numProcessors);
			System.out.println("Number of threads: " + numThreads);
			System.out.println("Execution time (seconds): " + (stop-start)/1.0E9);
		}
		catch (Exception exception) {			
			System.out.println(exception);
		}
    }
}