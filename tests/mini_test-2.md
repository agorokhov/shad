2.1 На куске кода программы отметьте, на каких серверах работают выделенные фрагменты:

    public class WordCount extends Configured implements Tool {

    	public static void main(String[] args) throws Exception {        ---
        	ToolRunner.run(new WordCount(), args);                         |
    	}                                                                ---

    	@Override
    	public int run(String[] args) throws Exception {
        	Configuration conf = this.getConf();                        --- 
        	Job job = new Job(conf);                                      |
	        ...                                                           |
        	int status = job.waitForCompletion(true) ? 0 : 1;             |
        	return status;                                                |
    	}                                                               ---

    	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        	@Override
        	public void setup(Context context)
                	throws IOException
        	{                                                          ---
				...                                                      |
        	}                                                          ---

        	@Override
        	public void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException
        	{                                                          ---   
				...                                                      |
        	}                                                          ---
    	}
	}

Варианты ответов: рабочий сервер (тот, где запускаете программу), мастер кластера, нода кластера, маппер, редьюсер.

=====

2.2 Дан кусок кода


    public class WordCount extends Configured implements Tool {
		public static int variable = 0;                          <---------

    	public static void main(String[] args) throws Exception {        
        	ToolRunner.run(new WordCount(), args);                       
    	}                                                                

    	@Override
    	public int run(String[] args) throws Exception {
        	Configuration conf = this.getConf();
			variable = 25;                                       <---------                      
        	Job job = new Job(conf);                                      
	        ...                                                           
        	int status = job.waitForCompletion(true) ? 0 : 1;             
        	return status;                                                
    	}                                                               

    	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        	@Override
        	public void setup(Context context)
                	throws IOException
        	{                                                          
				System.err.println(variable);                    <---------                                                       
        	}                                                          

        	@Override
        	public void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException
        	{                                                             
				...                                                      
        	}                                                          
    	}
	}

(В нем есть главный класс и переменная-член. В main() ей присваивается некое значение. В Mapper.setup() выводим её на печать.) Какое значение будет выведено?

=====

2.3 Какие методы кроме map() есть в классе Mapper и для чего их можно использовать (коротко, одного примера достаточно)?

=====

2.4 Какие методы кроме reduce() есть в классе Reducer и для чего их можно использовать (коротко, одного примера достаточно)?

=====

2.5 Ваша MapReduce программа должна на map-стадии читать строки, разделять их на слова и выдавать только те, которые начинаются на некоторую букву. Вы хотите, чтобы эту букву можно было задать в аргументах запуска программы. Как передать это значение в mapper?

=====

2.6 MapReduce программа отработала, но вы не поняли - успешно или нет. Как определить? Выходная директория есть, в ней какие-то файлы.

=====

2.7 Ваша MapReduce программа на map-стадии читает строки определенного формата. Правда, среди них могут быть и некорректные строки. Как определить, были ли такие строки и много ли их было? Программа не должна при этом завершаться с ошибкой, а выдавать результат, полученный для правильных строк.
