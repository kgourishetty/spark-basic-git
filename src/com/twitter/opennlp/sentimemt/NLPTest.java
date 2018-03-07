package com.twitter.opennlp.sentimemt;

public class NLPTest {
	
	public static void main(String[] args) {
		NLP.init();
		System.out.println(NLP.findSentiment("#spidermanhomecoming was everything I've ever wished and hoped for in a Spider-Man film! Perfectly acted, innocent and hilarious! "));
	}

}
