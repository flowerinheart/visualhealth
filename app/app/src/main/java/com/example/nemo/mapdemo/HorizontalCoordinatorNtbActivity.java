package com.example.nemo.mapdemo;

import android.app.Activity;
import android.graphics.Color;
import android.os.Bundle;
import android.support.design.widget.CollapsingToolbarLayout;
import android.support.design.widget.CoordinatorLayout;
import android.support.design.widget.Snackbar;
import android.support.v4.view.PagerAdapter;
import android.support.v4.view.ViewPager;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import android.widget.Toast;

import com.example.nemo.mapdemo.map.ChinaMapView;
import com.example.nemo.mapdemo.map.ProvinceData;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import devlight.io.library.ntb.NavigationTabBar;
import rx.Observable;
import rx.android.schedulers.AndroidSchedulers;
import rx.schedulers.Schedulers;

public class HorizontalCoordinatorNtbActivity extends Activity {

    ChinaMapView mapView = null;
    @Override
    protected void onCreate(final Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_horizontal_coordinator_ntb);
              initUI();
    }





    /**
     * 加载预设数据
     */
    public void loadMapData(ChinaMapView view) {

        parseDemoData()
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(
                        dataList -> view.setData(dataList),
                        throwable -> Toast.makeText(getBaseContext(), "加载演示数据失败！", Toast.LENGTH_SHORT).show()
                );
    }


    private Observable<List<ProvinceData>> parseDemoData() {

        return Observable.create(subscriber -> {

            List<ProvinceData> demoDataList = new ArrayList<>();
            try {
                InputStreamReader isr = new InputStreamReader(getBaseContext().getAssets().open("result.json"), "UTF-8");
                BufferedReader br = new BufferedReader(isr);
                String line;
                StringBuilder builder = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    builder.append(line);
                }
                br.close();
                isr.close();

                JSONObject root = new JSONObject(builder.toString());
                JSONArray dataList = root.optJSONArray("province");
                if (dataList != null && dataList.length() != 0) {
                    for (int i = 0; i < dataList.length(); i++) {

                        ProvinceData item = new ProvinceData();
                        JSONObject obj = dataList.optJSONObject(i);

                        item.setNumber(obj.optInt("number"));
                        item.setProvinceId(obj.optInt("provinceName"));
                        item.setProvinceId(obj.optInt("provinceId"));
                        demoDataList.add(item);
                    }
                }
            } catch (IOException | JSONException e) {
                e.printStackTrace();
            }

            subscriber.onNext(demoDataList);
            subscriber.onCompleted();
        });
    }


    private void initUI() {
        //init view pager
        final ViewPager viewPager = (ViewPager) findViewById(R.id.vp_horizontal_ntb);
    viewPager.setAdapter(new PagerAdapter() {
            @Override
            public int getCount() {
                return 5;
            }

            @Override
            public boolean isViewFromObject(final View view, final Object object) {
                return view.equals(object);
            }

            @Override
            public void destroyItem(final View container, final int position, final Object object) {
                ((ViewPager) container).removeView((View) object);
            }

            @Override
            public Object instantiateItem(final ViewGroup container, final int position) {
                View view = null;
                if(position == 0) {
                    view = LayoutInflater.from(
                            getBaseContext()).inflate(R.layout.map, null, false);


                    mapView = (ChinaMapView)findViewById(R.id.map_view);
//                    mapView.loadMapData(mapView);
                    container.addView(view);
                    //view.draw(view.);
                }else {
                    view = LayoutInflater.from(
                            getBaseContext()).inflate(R.layout.item_vp_list, null, false);

                    final RecyclerView recyclerView = (RecyclerView) view.findViewById(R.id.rv);
                    recyclerView.setHasFixedSize(true);
                    recyclerView.setLayoutManager(new LinearLayoutManager(
                                    getBaseContext(), LinearLayoutManager.VERTICAL, false
                            )
                    );
                    recyclerView.setAdapter(new RecycleAdapter());

                    container.addView(view);
                }
                return view;

            }
        });


        final String[] colors = getResources().getStringArray(R.array.default_preview);

        final NavigationTabBar navigationTabBar = (NavigationTabBar) findViewById(R.id.ntb_horizontal);
        final ArrayList<NavigationTabBar.Model> models = new ArrayList<>();
        models.add(
                new NavigationTabBar.Model.Builder(
                        getResources().getDrawable(R.drawable.map),
                        Color.parseColor(colors[0]))
                        .title("空气地图")
                        .build()
        );
        models.add(
                new NavigationTabBar.Model.Builder(
                        getResources().getDrawable(R.drawable.city),
                        Color.parseColor(colors[1]))
                        .title("City")
                        .build()
        );
        models.add(
                new NavigationTabBar.Model.Builder(
                        getResources().getDrawable(R.drawable.leaf),
                        Color.parseColor(colors[2]))
                        .title("discover")
                        .build()
        );
        models.add(
                new NavigationTabBar.Model.Builder(
                        getResources().getDrawable(R.drawable.ball),
                        Color.parseColor(colors[3]))
                        .title("Flag")
                        .build()
        );
        models.add(
                new NavigationTabBar.Model.Builder(
                        getResources().getDrawable(R.drawable.settings),
                        Color.parseColor(colors[4]))
                        .badgeTitle("ttt")
                        .title("Medal")
                        .build()
        );

        navigationTabBar.setModels(models);
        navigationTabBar.setViewPager(viewPager, 2);

        //IMPORTANT: ENABLE SCROLL BEHAVIOUR IN COORDINATOR LAYOUT
        navigationTabBar.setBehaviorEnabled(true);

        navigationTabBar.setOnTabBarSelectedIndexListener(new NavigationTabBar.OnTabBarSelectedIndexListener() {
            @Override
            public void onStartTabSelected(final NavigationTabBar.Model model, final int index) {
            }

            @Override
            public void onEndTabSelected(final NavigationTabBar.Model model, final int index) {
                model.hideBadge();
            }
        });
        navigationTabBar.setOnPageChangeListener(new ViewPager.OnPageChangeListener() {
            @Override
            public void onPageScrolled(final int position, final float positionOffset, final int positionOffsetPixels) {

            }

            @Override
            public void onPageSelected(final int position) {

            }

            @Override
            public void onPageScrollStateChanged(final int state) {

            }
        });

        final CoordinatorLayout coordinatorLayout = (CoordinatorLayout) findViewById(R.id.parent);
        findViewById(R.id.fab).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(final View v) {
                for (int i = 0; i < navigationTabBar.getModels().size(); i++) {
                    final NavigationTabBar.Model model = navigationTabBar.getModels().get(i);
                    navigationTabBar.postDelayed(new Runnable() {
                        @Override
                        public void run() {
                            final String title = String.valueOf(new Random().nextInt(15));
                            if (!model.isBadgeShowed()) {
                                model.setBadgeTitle(title);
                                model.showBadge();
                            } else model.updateBadgeTitle(title);
                        }
                    }, i * 100);
                }

                coordinatorLayout.postDelayed(new Runnable() {
                    @Override
                    public void run() {
                        final Snackbar snackbar = Snackbar.make(navigationTabBar, "Coordinator NTB", Snackbar.LENGTH_SHORT);
                        snackbar.getView().setBackgroundColor(Color.parseColor("#9b92b3"));
                        ((TextView) snackbar.getView().findViewById(R.id.snackbar_text))
                                .setTextColor(Color.parseColor("#f0f8ff"));
                        snackbar.show();
                    }
                }, 1000);
            }
        });

//        final CollapsingToolbarLayout collapsingToolbarLayout =
//                (CollapsingToolbarLayout) findViewById(R.id.toolbar);
//        collapsingToolbarLayout.setExpandedTitleColor(Color.parseColor("#009F90AF"));
//        collapsingToolbarLayout.setCollapsedTitleTextColor(Color.parseColor("#9f90af"));
    }

    public class RecycleAdapter extends RecyclerView.Adapter<RecycleAdapter.ViewHolder> {

        @Override
        public ViewHolder onCreateViewHolder(final ViewGroup parent, final int viewType) {
            final View view = LayoutInflater.from(getBaseContext()).inflate(R.layout.item_list, parent, false);
            return new ViewHolder(view);
        }

        @Override
        public void onBindViewHolder(final ViewHolder holder, final int position) {
            holder.txt.setText(String.format("Navigation Item #%d", position));
        }

        @Override
        public int getItemCount() {
            return 20;
        }

        public class ViewHolder extends RecyclerView.ViewHolder {

            public TextView txt;

            public ViewHolder(final View itemView) {
                super(itemView);
                txt = (TextView) itemView.findViewById(R.id.txt_vp_item_list);
            }
        }
    }
}
