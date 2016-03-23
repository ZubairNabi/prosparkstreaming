#!/bin/bash
        for i in `seq 1 10`;
        do
		p=/Users/zubairnabi/Downloads/dummy/${i}.gz
                echo ${p}
		touch -c ${p}
		sleep 1
      	done    
