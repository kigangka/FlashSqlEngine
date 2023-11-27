package io.github.xingchuan.sql.exception;

/**
 * 自定义异常处理
 *
 * @author kisang
 * @Date 2023/11/27 14:00:27
 */
public class FlashSqlEngineException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private String msg;
    private int code = 500;

    public FlashSqlEngineException(String msg) {
        super(msg);
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }


}
