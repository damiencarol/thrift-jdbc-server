package driver;

import java.sql.SQLWarning;
import java.util.List;

import driver.io.CCSQLWarning;

public class CrystalWarning {

    public static SQLWarning buildFromList(List<CCSQLWarning> warnings) {
        if (warnings==null){
            return null;
        }
        if (warnings.isEmpty()){
            return null;
        }
        SQLWarning ret_warn = buildFromPart(warnings.get(0));
        SQLWarning next = ret_warn;
        for (int i = 1; i < warnings.size(); i++) {
            next.setNextWarning(buildFromPart(warnings.get(i)));
            next = next.getNextWarning();
        }
        return ret_warn;
    }
    public static SQLWarning buildFromPart(CCSQLWarning warning) {
        return new SQLWarning(warning.reason, warning.state, warning.vendorCode);
    }

}
