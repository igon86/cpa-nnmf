function s = generAscanner(dimx,dimy,sparsity,file,startingRow)
    %S = sparse(zeros(dimx,dimy));
    dim = dimx*dimy;
    numelem = ceil(dim/sparsity)
    fprintf(1,'dim: %d \nnumelem: %d\n',dim,numelem);
    %fflush(1);
    fid = fopen(file,'w');

    s=0;
    tic
    for i =1: dimx
        for j=1:dimy
            p = rand();
            %a = ceil(rand()*dimx);
            %b = ceil(rand()*dimy);
            if p <= ((numelem-s)/dim)
                %a colonna b riga
                %[a,b] = ind2sub([dimx,dimy],i);

                x = 0;
                while x <= 0
                    x = randn()+100;
                end
                s = s+1;
                fprintf(fid,'%d#%d#%f\n',i+startingRow,j,x);
            end
%             if mod(i,100)==0
%                 fprintf(1,'%d\n',i);
%             end
        end
    end
    toc
end